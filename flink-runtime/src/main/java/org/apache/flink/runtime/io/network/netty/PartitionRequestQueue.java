/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.NetworkSequenceViewReader;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.netty.NettyMessage.ErrorResponse;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.PartitionRequestListener;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;

import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

import static org.apache.flink.runtime.io.network.netty.NettyMessage.BufferResponse;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A nonEmptyReader of partition queues, which listens for channel writability changed events before
 * writing and flushing {@link Buffer} instances.
 */
//分区队列的nonEmptyReader，它在写入和刷新缓冲区实例之前侦听通道可写性更改事件。
class PartitionRequestQueue extends ChannelInboundHandlerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionRequestQueue.class);

    private final ChannelFutureListener writeListener =
            new WriteAndFlushNextMessageIfPossibleListener();

    /** The readers which are already enqueued available for transferring data. */
    //已入队的读取器可用于传输数据。
    private final ArrayDeque<NetworkSequenceViewReader> availableReaders = new ArrayDeque<>();

    /** All the readers created for the consumers' partition requests. */
    //为消费者的分区请求创建的所有读视图
    private final ConcurrentMap<InputChannelID, NetworkSequenceViewReader> allReaders =
            new ConcurrentHashMap<>();

    private boolean fatalError;

    private ChannelHandlerContext ctx;

    @Override
    public void channelRegistered(final ChannelHandlerContext ctx) throws Exception {
        if (this.ctx == null) {
            this.ctx = ctx;
        }

        super.channelRegistered(ctx);
    }

    //通知读取器非空
    void notifyReaderNonEmpty(final NetworkSequenceViewReader reader) {
        // The notification might come from the same thread. For the initial writes this
        // might happen before the reader has set its reference to the view, because
        // creating the queue and the initial notification happen in the same method call.
        // This can be resolved by separating the creation of the view and allowing
        // notifications.
        //通知可能来自同一个线程。对于初始写入，这可能发生在读取器将其引用设置为视图之前，
        // 因为创建队列和初始通知发生在同一方法调用中。这可以通过分离视图的创建和允许通知来解决。

        // TODO This could potentially have a bad performance impact as in the
        // worst case (network consumes faster than the producer) each buffer
        // will trigger a separate event loop task being scheduled.
        //这可能会产生不良的性能影响，因为在最坏的情况下 (网络消耗比生产者更快)，每个缓冲区将触发一个单独的事件循环任务被调度。
        ctx.executor().execute(() -> ctx.pipeline()
                //调用userEventTriggered(...)方法
                .fireUserEventTriggered(reader));
    }

    /**
     * Try to enqueue the reader once receiving credit notification from the consumer or receiving
     * non-empty reader notification from the producer.
     *
     * <p>NOTE: Only one thread would trigger the actual enqueue after checking the reader's
     * availability, so there is no race condition here.
     */
    //尝试在收到来自消费者的信用通知或收到来自生产者的非空读取器通知后将读取器排队。
    //注意: 只有一个线程会在检查读者的可用性后触发实际的队列，所以这里没有竞争条件。
    private void enqueueAvailableReader(final NetworkSequenceViewReader reader) throws Exception {
        if (reader.isRegisteredAsAvailable()) {
            return;
        }

        ResultSubpartitionView.AvailabilityWithBacklog availabilityWithBacklog =
                reader.getAvailabilityAndBacklog();
        if (!availabilityWithBacklog.isAvailable()) {
            int backlog = availabilityWithBacklog.getBacklog();
            if (backlog > 0 && reader.needAnnounceBacklog()) {
                announceBacklog(reader, backlog);
            }
            return;
        }

        // Queue an available reader for consumption. If the queue is empty,
        // we try trigger the actual write. Otherwise this will be handled by
        // the writeAndFlushNextMessageIfPossible calls.
        //将可用的读取器排队以供使用。如果队列为空，我们尝试触发实际写入。
        // 否则，这将由writeandflushnextmessageif可能的调用处理。
        boolean triggerWrite = availableReaders.isEmpty();
        registerAvailableReader(reader);

        if (triggerWrite) {
            //如果可能，写入并刷新下一条消息
            writeAndFlushNextMessageIfPossible(ctx.channel());
        }
    }

    /**
     * Accesses internal state to verify reader registration in the unit tests.
     *
     * <p><strong>Do not use anywhere else!</strong>
     *
     * @return readers which are enqueued available for transferring data
     */
    @VisibleForTesting
    ArrayDeque<NetworkSequenceViewReader> getAvailableReaders() {
        return availableReaders;
    }

    public void notifyReaderCreated(final NetworkSequenceViewReader reader) {
        allReaders.put(reader.getReceiverId(), reader);
    }

    public void cancel(InputChannelID receiverId) {
        ctx.pipeline().fireUserEventTriggered(receiverId);
    }

    public void close() throws IOException {
        if (ctx != null) {
            ctx.channel().close();
        }

        releaseAllResources();
    }

    /**
     * Adds unannounced credits from the consumer or resumes data consumption after an exactly-once
     * checkpoint and enqueues the corresponding reader for this consumer (if not enqueued yet).
     *
     * @param receiverId The input channel id to identify the consumer.
     * @param operation The operation to be performed (add credit or resume data consumption).
     */
    //添加来自消费者的未宣布积分或在一次检查点后恢复数据消费，并为该消费者排队相应的读取器 (如果尚未排队)。
    void addCreditOrResumeConsumption(
            InputChannelID receiverId, Consumer<NetworkSequenceViewReader> operation)
            throws Exception {
        if (fatalError) {
            return;
        }

        NetworkSequenceViewReader reader = obtainReader(receiverId);

        operation.accept(reader);
        //查询可用读取
        enqueueAvailableReader(reader);
    }

    void acknowledgeAllRecordsProcessed(InputChannelID receiverId) {
        if (fatalError) {
            return;
        }

        obtainReader(receiverId).acknowledgeAllRecordsProcessed();
    }

    //通知新缓冲区大小
    void notifyNewBufferSize(InputChannelID receiverId, int newBufferSize) {
        if (fatalError) {
            return;
        }

        // It is possible to receive new buffer size before the reader would be created since the
        // downstream task could calculate buffer size even using the data from one channel but it
        // sends new buffer size into all upstream even if they don't ready yet. In this case, just
        // ignore the new buffer size.
        //可以在创建读取器之前接收新的缓冲区大小，因为下游任务即使使用来自一个通道的数据也可以计算缓冲区大小，
        // 但是它将新的缓冲区大小发送到所有上游，即使它们还没有准备好。在这种情况下，只需忽略新的缓冲区大小。
        NetworkSequenceViewReader reader = allReaders.get(receiverId);
        if (reader != null) {
            reader.notifyNewBufferSize(newBufferSize);
        }
    }

    /**
     * Notify the id of required segment from the consumer.
     *
     * @param receiverId The input channel id to identify the consumer.
     * @param subpartitionId The id of the corresponding subpartition.
     * @param segmentId The id of required segment.
     */
    void notifyRequiredSegmentId(InputChannelID receiverId, int subpartitionId, int segmentId) {
        if (fatalError) {
            return;
        }
        NetworkSequenceViewReader reader = allReaders.get(receiverId);
        if (reader != null) {
            reader.notifyRequiredSegmentId(subpartitionId, segmentId);
        }
    }

    NetworkSequenceViewReader obtainReader(InputChannelID receiverId) {
        NetworkSequenceViewReader reader = allReaders.get(receiverId);
        if (reader == null) {
            throw new IllegalStateException(
                    "No reader for receiverId = " + receiverId + " exists.");
        }

        return reader;
    }

    /**
     * Announces remaining backlog to the consumer after the available data notification or data
     * consumption resumption.
     */
    //在可用数据通知或数据消费恢复后，向消费者通告剩余积压。
    private void announceBacklog(NetworkSequenceViewReader reader, int backlog) {
        checkArgument(backlog > 0, "Backlog must be positive.");

        NettyMessage.BacklogAnnouncement announcement =
                new NettyMessage.BacklogAnnouncement(backlog, reader.getReceiverId());
        ctx.channel()
                .writeAndFlush(announcement)
                .addListener(
                        (ChannelFutureListener)
                                future -> {
                                    if (!future.isSuccess()) {
                                        onChannelFutureFailure(future);
                                    }
                                });
    }

    //用户事件已触发
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object msg) throws Exception {
        // The user event triggered event loop callback is used for thread-safe
        // hand over of reader queues and cancelled producers.
        //用户事件触发的事件循环回调用于读取器队列和取消的生产者的线程安全移交。

        if (msg instanceof NetworkSequenceViewReader) {
            //PartitionRequestQueue会将reader放到可用reader队列里，然后触发数据的读取操作。
            enqueueAvailableReader((NetworkSequenceViewReader) msg);
        } else if (msg.getClass() == InputChannelID.class) {
            // Release partition view that get a cancel request.
            InputChannelID toCancel = (InputChannelID) msg;

            // remove reader from queue of available readers
            availableReaders.removeIf(reader -> reader.getReceiverId().equals(toCancel));

            // remove reader from queue of all readers and release its resource
            final NetworkSequenceViewReader toRelease = allReaders.remove(toCancel);
            if (toRelease != null) {
                releaseViewReader(toRelease);
            }
        } else if (msg instanceof PartitionRequestListener) {
            PartitionRequestListener partitionRequestListener = (PartitionRequestListener) msg;

            // Send partition not found message to the downstream task when the listener is timeout.
            final ResultPartitionID resultPartitionId =
                    partitionRequestListener.getResultPartitionId();
            final InputChannelID inputChannelId = partitionRequestListener.getReceiverId();
            availableReaders.remove(partitionRequestListener.getViewReader());
            allReaders.remove(inputChannelId);
            try {
                ctx.writeAndFlush(
                        new NettyMessage.ErrorResponse(
                                new PartitionNotFoundException(resultPartitionId), inputChannelId));
            } catch (Exception e) {
                LOG.warn(
                        "Write partition not found exception to {} for result partition {} fail",
                        inputChannelId,
                        resultPartitionId,
                        e);
            }
        } else {
            ctx.fireUserEventTriggered(msg);
        }
    }

    //通道可写性已更改
    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        writeAndFlushNextMessageIfPossible(ctx.channel());
    }

    //如果可能，写入并刷新下一条消息
    private void writeAndFlushNextMessageIfPossible(final Channel channel) throws IOException {
        if (fatalError || !channel.isWritable()) {
            return;
        }

        // The logic here is very similar to the combined input gate and local
        // input channel logic. You can think of this class acting as the input
        // gate and the consumed views as the local input channels.

        BufferAndAvailability next = null;
        int nextSubpartitionId = -1;
        try {
            while (true) {
                NetworkSequenceViewReader reader = pollAvailableReader();

                // No queue with available data. We allow this here, because
                // of the write callbacks that are executed after each write.
                //没有包含可用数据的队列。我们在这里允许这样做，因为在每次写入之后执行写入回调。
                if (reader == null) {
                    return;
                }

                nextSubpartitionId = reader.peekNextBufferSubpartitionId();

                //读取数据Buffer
                //读取数据过程中可知会判断buffer类型是否是数据Buffer并且服务端信用值会减去1判断是否大于0，
                //标识NettyClient端是否有可用Buffer来接收服务端发送的数据。
                next = reader.getNextBuffer();
                if (next == null) {
                    if (!reader.isReleased()) {
                        continue;
                    }

                    Throwable cause = reader.getFailureCause();
                    if (cause != null) {
                        ErrorResponse msg = new ErrorResponse(cause, reader.getReceiverId());

                        ctx.writeAndFlush(msg);
                    }
                } else {
                    // This channel was now removed from the available reader queue.
                    // We re-add it into the queue if it is still available
                    //此通道现已从可用读取器队列中删除。如果它仍然可用，我们将其重新添加到队列中
                    if (next.moreAvailable()) {
                        //注册可用的读取器
                        registerAvailableReader(reader);
                    }

                    BufferResponse msg =
                            new BufferResponse(
                                    next.buffer(),
                                    next.getSequenceNumber(),
                                    reader.getReceiverId(),
                                    nextSubpartitionId,
                                    next.buffersInBacklog());

                    // Write and flush and wait until this is done before
                    // trying to continue with the next buffer.
                    //写和刷新并等待，直到这在尝试继续下一个缓冲区之前完成。

                    //将数据发送给下游NettyClient端。
                    channel.writeAndFlush(msg).addListener(writeListener);

                    return;
                }
            }
        } catch (Throwable t) {
            if (next != null) {
                next.buffer().recycleBuffer();
            }

            throw new IOException(t.getMessage(), t);
        }
    }

    private void registerAvailableReader(NetworkSequenceViewReader reader) {
        availableReaders.add(reader);
        reader.setRegisteredAsAvailable(true);
    }

    @Nullable
    private NetworkSequenceViewReader pollAvailableReader() {
        NetworkSequenceViewReader reader = availableReaders.poll();
        if (reader != null) {
            reader.setRegisteredAsAvailable(false);
        }
        return reader;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        releaseAllResources();

        ctx.fireChannelInactive();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        handleException(ctx.channel(), cause);
    }

    private void handleException(Channel channel, Throwable cause) throws IOException {
        LOG.error(
                "Encountered error while consuming partitions (connection to {})",
                channel.remoteAddress(),
                cause);

        fatalError = true;
        releaseAllResources();

        if (channel.isActive()) {
            channel.writeAndFlush(new ErrorResponse(cause))
                    .addListener(ChannelFutureListener.CLOSE);
        }
    }

    private void releaseAllResources() throws IOException {
        // note: this is only ever executed by one thread: the Netty IO thread!
        for (NetworkSequenceViewReader reader : allReaders.values()) {
            releaseViewReader(reader);
        }

        availableReaders.clear();
        allReaders.clear();
    }

    private void releaseViewReader(NetworkSequenceViewReader reader) throws IOException {
        reader.setRegisteredAsAvailable(false);
        reader.releaseAllResources();
    }

    private void onChannelFutureFailure(ChannelFuture future) throws Exception {
        if (future.cause() != null) {
            handleException(future.channel(), future.cause());
        } else {
            handleException(
                    future.channel(), new IllegalStateException("Sending cancelled by user."));
        }
    }

    public void notifyPartitionRequestTimeout(PartitionRequestListener partitionRequestListener) {
        ctx.pipeline().fireUserEventTriggered(partitionRequestListener);
    }

    // This listener is called after an element of the current nonEmptyReader has been
    // flushed. If successful, the listener triggers further processing of the
    // queues.
    //在刷新当前nonEmptyReader的元素后调用此侦听器。如果成功，则监听器触发队列的进一步处理。
    private class WriteAndFlushNextMessageIfPossibleListener implements ChannelFutureListener {

        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            try {
                if (future.isSuccess()) {
                    writeAndFlushNextMessageIfPossible(future.channel());
                } else {
                    onChannelFutureFailure(future);
                }
            } catch (Throwable t) {
                handleException(future.channel(), t);
            }
        }
    }
}
