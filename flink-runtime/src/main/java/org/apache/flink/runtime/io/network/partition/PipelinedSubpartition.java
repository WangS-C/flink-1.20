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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumerWithPartialRecordLength;
import org.apache.flink.runtime.io.network.logger.NetworkActionsLogger;

import org.apache.flink.shaded.guava31.com.google.common.collect.Iterators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A pipelined in-memory only subpartition, which can be consumed once.
 *
 * <p>Whenever {@link ResultSubpartition#add(BufferConsumer)} adds a finished {@link BufferConsumer}
 * or a second {@link BufferConsumer} (in which case we will assume the first one finished), we will
 * {@link PipelinedSubpartitionView#notifyDataAvailable() notify} a read view created via {@link
 * ResultSubpartition#createReadView(BufferAvailabilityListener)} of new data availability. Except
 * by calling {@link #flush()} explicitly, we always only notify when the first finished buffer
 * turns up and then, the reader has to drain the buffers via {@link #pollBuffer()} until its return
 * value shows no more buffers being available. This results in a buffer queue which is either empty
 * or has an unfinished {@link BufferConsumer} left from which the notifications will eventually
 * start again.
 *
 * <p>Explicit calls to {@link #flush()} will force this {@link
 * PipelinedSubpartitionView#notifyDataAvailable() notification} for any {@link BufferConsumer}
 * present in the queue.
 */
//仅在内存中的管道子分区，可以使用一次。
//每当ResultSubpartition. add(BufferConsumer)添加一个已完成的BufferConsumer或第二个BufferConsumer
// （在这种情况下，我们将假设第一个已完成），我们将notify通过
// ResultSubpartition. createReadView(BufferAvailabilityListener)创建的读取视图新数据的可用性。
// 除了显式调用flush()之外，我们总是只在第一个完成的缓冲区出现时发出通知，然后读取器必须通过pollBuffer()耗尽缓冲区，
// 直到其返回值显示没有更多缓冲区可用。这会导致缓冲区队列为空或有未完成的BufferConsumer ，通知最终将从中重新开始。
//对flush()显式调用将强制向队列中存在的任何BufferConsumer发出此notification 。
public class PipelinedSubpartition extends ResultSubpartition implements ChannelStateHolder {

    private static final Logger LOG = LoggerFactory.getLogger(PipelinedSubpartition.class);

    private static final int DEFAULT_PRIORITY_SEQUENCE_NUMBER = -1;

    // ------------------------------------------------------------------------

    /**
     * Number of exclusive credits per input channel at the downstream tasks configured by {@link
     * org.apache.flink.configuration.NettyShuffleEnvironmentOptions#NETWORK_BUFFERS_PER_CHANNEL}.
     */
    //由org. apache. flink. configuration.
    // NettyShuffleEnvironmentOptions. NETWORK_BUFFERS_PER_CHANNEL配置的下游任务中每个输入通道的独占积分数。
    private final int receiverExclusiveBuffersPerChannel;

    /** All buffers of this subpartition. Access to the buffers is synchronized on this object. */
    //此子分区的所有缓冲区。在此对象上同步对缓冲区的访问。
    //用于缓存写入的数据，等待下游Task消费buffers里的数据。
    final PrioritizedDeque<BufferConsumerWithPartialRecordLength> buffers =
            new PrioritizedDeque<>();

    /** The number of non-event buffers currently in this subpartition. */
    //当前此子分区中的非事件缓冲区的数量。
    @GuardedBy("buffers")
    private int buffersInBacklog;

    /** The read view to consume this subpartition. */
    //要使用此子分区的读取视图
    //读视图，该结果子分区数据消费行为的封装。
    PipelinedSubpartitionView readView;

    /** Flag indicating whether the subpartition has been finished. */
    //指示子分区是否已完成的标志。
    private boolean isFinished;

    @GuardedBy("buffers")
    private boolean flushRequested;

    /** Flag indicating whether the subpartition has been released. */
    //指示子分区是否已释放的标志。
    volatile boolean isReleased;

    /** The total number of buffers (both data and event buffers). */
    //缓冲区总数（数据缓冲区和事件缓冲区）。
    private long totalNumberOfBuffers;

    /** The total number of bytes (both data and event buffers). */
    //字节总数（数据缓冲区和事件缓冲区）。
    private long totalNumberOfBytes;

    /** Writes in-flight data. */
    //在检查点/ 保存点期间写入数据
    private ChannelStateWriter channelStateWriter;

    private int bufferSize = Integer.MAX_VALUE;

    /** The channelState Future of unaligned checkpoint. */
    //未对齐检查点的channelState Future
    @GuardedBy("buffers")
    private CompletableFuture<List<Buffer>> channelStateFuture;

    /**
     * It is the checkpointId corresponding to channelStateFuture. And It should be always update
     * with {@link #channelStateFuture}.
     */
    //就是channelStateFuture对应的checkpointId。并且它应该始终使用channelStateFuture进行更新。
    @GuardedBy("buffers")
    private long channelStateCheckpointId;

    /**
     * Whether this subpartition is blocked (e.g. by exactly once checkpoint) and is waiting for
     * resumption.
     */
    //该子分区是否被阻塞（例如，被恰好一次检查点阻塞）并正在等待恢复。
    @GuardedBy("buffers")
    boolean isBlocked = false;

    int sequenceNumber = 0;

    // ------------------------------------------------------------------------

    PipelinedSubpartition(
            int index, int receiverExclusiveBuffersPerChannel, ResultPartition parent) {
        super(index, parent);

        checkArgument(
                receiverExclusiveBuffersPerChannel >= 0,
                "Buffers per channel must be non-negative.");
        this.receiverExclusiveBuffersPerChannel = receiverExclusiveBuffersPerChannel;
    }

    @Override
    public void setChannelStateWriter(ChannelStateWriter channelStateWriter) {
        checkState(this.channelStateWriter == null, "Already initialized");
        this.channelStateWriter = checkNotNull(channelStateWriter);
    }

    @Override
    public int add(BufferConsumer bufferConsumer, int partialRecordLength) {
        return add(bufferConsumer, partialRecordLength, false);
    }

    public boolean isSupportChannelStateRecover() {
        return true;
    }

    @Override
    public int finish() throws IOException {
        BufferConsumer eventBufferConsumer =
                EventSerializer.toBufferConsumer(EndOfPartitionEvent.INSTANCE, false);
        add(eventBufferConsumer, 0, true);
        LOG.debug("{}: Finished {}.", parent.getOwningTaskName(), this);
        return eventBufferConsumer.getWrittenBytes();
    }

    private int add(BufferConsumer bufferConsumer, int partialRecordLength, boolean finish) {
        checkNotNull(bufferConsumer);

        final boolean notifyDataAvailable;
        int prioritySequenceNumber = DEFAULT_PRIORITY_SEQUENCE_NUMBER;
        int newBufferSize;
        synchronized (buffers) {
            if (isFinished || isReleased) {
                bufferConsumer.close();
                return ADD_BUFFER_ERROR_CODE;
            }

            // Add the bufferConsumer and update the stats
            //添加bufferConsumer并更新统计信息
            if (addBuffer(bufferConsumer, partialRecordLength)) {
                prioritySequenceNumber = sequenceNumber;
            }
            updateStatistics(bufferConsumer);
            increaseBuffersInBacklog(bufferConsumer);
            notifyDataAvailable = finish || shouldNotifyDataAvailable();

            isFinished |= finish;
            newBufferSize = bufferSize;
        }

        notifyPriorityEvent(prioritySequenceNumber);
        if (notifyDataAvailable) {
            //通知可用数据
            notifyDataAvailable();
        }

        return newBufferSize;
    }

    @GuardedBy("buffers")
    private boolean addBuffer(BufferConsumer bufferConsumer, int partialRecordLength) {
        assert Thread.holdsLock(buffers);
        if (bufferConsumer.getDataType().hasPriority()) {
            return processPriorityBuffer(bufferConsumer, partialRecordLength);
        } else if (Buffer.DataType.TIMEOUTABLE_ALIGNED_CHECKPOINT_BARRIER
                == bufferConsumer.getDataType()) {
            processTimeoutableCheckpointBarrier(bufferConsumer);
        }
        //调用add
        buffers.add(new BufferConsumerWithPartialRecordLength(bufferConsumer, partialRecordLength));
        return false;
    }

    @GuardedBy("buffers")
    private boolean processPriorityBuffer(BufferConsumer bufferConsumer, int partialRecordLength) {
        buffers.addPriorityElement(
                new BufferConsumerWithPartialRecordLength(bufferConsumer, partialRecordLength));
        final int numPriorityElements = buffers.getNumPriorityElements();

        CheckpointBarrier barrier = parseCheckpointBarrier(bufferConsumer);
        if (barrier != null) {
            checkState(
                    barrier.getCheckpointOptions().isUnalignedCheckpoint(),
                    "Only unaligned checkpoints should be priority events");
            final Iterator<BufferConsumerWithPartialRecordLength> iterator = buffers.iterator();
            Iterators.advance(iterator, numPriorityElements);
            List<Buffer> inflightBuffers = new ArrayList<>();
            while (iterator.hasNext()) {
                BufferConsumer buffer = iterator.next().getBufferConsumer();

                if (buffer.isBuffer()) {
                    try (BufferConsumer bc = buffer.copy()) {
                        inflightBuffers.add(bc.build());
                    }
                }
            }
            if (!inflightBuffers.isEmpty()) {
                channelStateWriter.addOutputData(
                        barrier.getId(),
                        subpartitionInfo,
                        ChannelStateWriter.SEQUENCE_NUMBER_UNKNOWN,
                        inflightBuffers.toArray(new Buffer[0]));
            }
        }
        return needNotifyPriorityEvent();
    }

    // It is just called after add priorityEvent.
    //它只是在添加priorityEvent之后调用。
    @GuardedBy("buffers")
    private boolean needNotifyPriorityEvent() {
        assert Thread.holdsLock(buffers);
        // if subpartition is blocked then downstream doesn't expect any notifications
        //如果子分区被阻止，则下游不会收到任何通知
        return buffers.getNumPriorityElements() == 1 && !isBlocked;
    }

    @GuardedBy("buffers")
    private void processTimeoutableCheckpointBarrier(BufferConsumer bufferConsumer) {
        CheckpointBarrier barrier = parseAndCheckTimeoutableCheckpointBarrier(bufferConsumer);
        channelStateWriter.addOutputDataFuture(
                barrier.getId(),
                subpartitionInfo,
                ChannelStateWriter.SEQUENCE_NUMBER_UNKNOWN,
                createChannelStateFuture(barrier.getId()));
    }

    @GuardedBy("buffers")
    private CompletableFuture<List<Buffer>> createChannelStateFuture(long checkpointId) {
        assert Thread.holdsLock(buffers);
        if (channelStateFuture != null) {
            completeChannelStateFuture(
                    null,
                    new IllegalStateException(
                            String.format(
                                    "%s has uncompleted channelStateFuture of checkpointId=%s, but it received "
                                            + "a new timeoutable checkpoint barrier of checkpointId=%s, it maybe "
                                            + "a bug due to currently not supported concurrent unaligned checkpoint.",
                                    this, channelStateCheckpointId, checkpointId)));
        }
        channelStateFuture = new CompletableFuture<>();
        channelStateCheckpointId = checkpointId;
        return channelStateFuture;
    }

    @GuardedBy("buffers")
    private void completeChannelStateFuture(List<Buffer> channelResult, Throwable e) {
        assert Thread.holdsLock(buffers);
        if (e != null) {
            channelStateFuture.completeExceptionally(e);
        } else {
            channelStateFuture.complete(channelResult);
        }
        channelStateFuture = null;
    }

    @GuardedBy("buffers")
    private boolean isChannelStateFutureAvailable(long checkpointId) {
        assert Thread.holdsLock(buffers);
        return channelStateFuture != null && channelStateCheckpointId == checkpointId;
    }

    private CheckpointBarrier parseAndCheckTimeoutableCheckpointBarrier(
            BufferConsumer bufferConsumer) {
        CheckpointBarrier barrier = parseCheckpointBarrier(bufferConsumer);
        checkArgument(barrier != null, "Parse the timeoutable Checkpoint Barrier failed.");
        checkState(
                barrier.getCheckpointOptions().isTimeoutable()
                        && Buffer.DataType.TIMEOUTABLE_ALIGNED_CHECKPOINT_BARRIER
                                == bufferConsumer.getDataType());
        return barrier;
    }

    @Override
    public void alignedBarrierTimeout(long checkpointId) throws IOException {
        int prioritySequenceNumber = DEFAULT_PRIORITY_SEQUENCE_NUMBER;
        synchronized (buffers) {
            // The checkpoint barrier has sent to downstream, so nothing to do.
            //检查点屏障已发送到下游，所以没有什么可做的。
            if (!isChannelStateFutureAvailable(checkpointId)) {
                return;
            }

            // 1. find inflightBuffers and timeout the aligned barrier to unaligned barrier
            //查找inflightBuffers并将对齐的屏障超时到未对齐的屏障
            List<Buffer> inflightBuffers = new ArrayList<>();
            try {
                if (findInflightBuffersAndMakeBarrierToPriority(checkpointId, inflightBuffers)) {
                    prioritySequenceNumber = sequenceNumber;
                }
            } catch (IOException e) {
                inflightBuffers.forEach(Buffer::recycleBuffer);
                completeChannelStateFuture(null, e);
                throw e;
            }

            // 2. complete the channelStateFuture
            //2.完成channelstateweuture
            completeChannelStateFuture(inflightBuffers, null);
        }

        // 3. notify downstream read barrier, it must be called outside the buffers_lock to avoid
        // the deadlock.
        //3.通知下游读屏障，它必须在buffers_lock之外调用以避免死锁。
        notifyPriorityEvent(prioritySequenceNumber);
    }

    @Override
    public void abortCheckpoint(long checkpointId, CheckpointException cause) {
        synchronized (buffers) {
            if (isChannelStateFutureAvailable(checkpointId)) {
                completeChannelStateFuture(null, cause);
            }
        }
    }

    @GuardedBy("buffers")
    private boolean findInflightBuffersAndMakeBarrierToPriority(
            long checkpointId, List<Buffer> inflightBuffers) throws IOException {
        // 1. record the buffers before barrier as inflightBuffers
        //1.将屏障前的缓冲区记录为inflightBuffers
        final int numPriorityElements = buffers.getNumPriorityElements();
        final Iterator<BufferConsumerWithPartialRecordLength> iterator = buffers.iterator();
        Iterators.advance(iterator, numPriorityElements);

        BufferConsumerWithPartialRecordLength element = null;
        CheckpointBarrier barrier = null;
        while (iterator.hasNext()) {
            BufferConsumerWithPartialRecordLength next = iterator.next();
            BufferConsumer bufferConsumer = next.getBufferConsumer();

            if (Buffer.DataType.TIMEOUTABLE_ALIGNED_CHECKPOINT_BARRIER
                    == bufferConsumer.getDataType()) {
                barrier = parseAndCheckTimeoutableCheckpointBarrier(bufferConsumer);
                // It may be an aborted barrier
                //它可能是一个中止的障碍
                if (barrier.getId() != checkpointId) {
                    continue;
                }
                element = next;
                break;
            } else if (bufferConsumer.isBuffer()) {
                try (BufferConsumer bc = bufferConsumer.copy()) {
                    inflightBuffers.add(bc.build());
                }
            }
        }

        // 2. Make the barrier to be priority
        //2.使屏障优先
        checkNotNull(
                element, "The checkpoint barrier=%d don't find in %s.", checkpointId, toString());
        makeBarrierToPriority(element, barrier);

        return needNotifyPriorityEvent();
    }

    private void makeBarrierToPriority(
            BufferConsumerWithPartialRecordLength oldElement, CheckpointBarrier barrier)
            throws IOException {
        buffers.getAndRemove(oldElement::equals);
        buffers.addPriorityElement(
                new BufferConsumerWithPartialRecordLength(
                        EventSerializer.toBufferConsumer(barrier.asUnaligned(), true), 0));
    }

    @Nullable
    private CheckpointBarrier parseCheckpointBarrier(BufferConsumer bufferConsumer) {
        CheckpointBarrier barrier;
        try (BufferConsumer bc = bufferConsumer.copy()) {
            Buffer buffer = bc.build();
            try {
                final AbstractEvent event =
                        EventSerializer.fromBuffer(buffer, getClass().getClassLoader());
                barrier = event instanceof CheckpointBarrier ? (CheckpointBarrier) event : null;
            } catch (IOException e) {
                throw new IllegalStateException(
                        "Should always be able to deserialize in-memory event", e);
            } finally {
                buffer.recycleBuffer();
            }
        }
        return barrier;
    }

    @Override
    public void release() {
        // view reference accessible outside the lock, but assigned inside the locked scope
        //视图引用可在锁外部访问，但在锁定范围内分配
        final PipelinedSubpartitionView view;

        synchronized (buffers) {
            if (isReleased) {
                return;
            }

            // Release all available buffers
            //释放所有可用缓冲区
            for (BufferConsumerWithPartialRecordLength buffer : buffers) {
                buffer.getBufferConsumer().close();
            }
            buffers.clear();

            if (channelStateFuture != null) {
                IllegalStateException exception =
                        new IllegalStateException("The PipelinedSubpartition is released");
                completeChannelStateFuture(null, exception);
            }

            view = readView;
            readView = null;

            // Make sure that no further buffers are added to the subpartition
            //确保没有进一步的缓冲区添加到子分区
            isReleased = true;
        }

        LOG.debug("{}: Released {}.", parent.getOwningTaskName(), this);

        if (view != null) {
            view.releaseAllResources();
        }
    }

    @Nullable
    BufferAndBacklog pollBuffer() {
        synchronized (buffers) {
            if (isBlocked) {
                return null;
            }

            Buffer buffer = null;

            if (buffers.isEmpty()) {
                flushRequested = false;
            }

            while (!buffers.isEmpty()) {
                //获取buffers队列的数据
                BufferConsumerWithPartialRecordLength bufferConsumerWithPartialRecordLength =
                        buffers.peek();
                BufferConsumer bufferConsumer =
                        bufferConsumerWithPartialRecordLength.getBufferConsumer();
                if (Buffer.DataType.TIMEOUTABLE_ALIGNED_CHECKPOINT_BARRIER
                        == bufferConsumer.getDataType()) {
                    completeTimeoutableCheckpointBarrier(bufferConsumer);
                }
                buffer = buildSliceBuffer(bufferConsumerWithPartialRecordLength);

                checkState(
                        bufferConsumer.isFinished() || buffers.size() == 1,
                        "When there are multiple buffers, an unfinished bufferConsumer can not be at the head of the buffers queue.");

                if (buffers.size() == 1) {
                    // turn off flushRequested flag if we drained all the available data
                    //如果我们耗尽了所有可用数据，请关闭flushRequested标志
                    flushRequested = false;
                }

                if (bufferConsumer.isFinished()) {
                    requireNonNull(buffers.poll()).getBufferConsumer().close();
                    //从自己的buffers队列里获取数据后会将自己的buffersInBacklog变量减1，代表结果子分区数据积压数减1。
                    decreaseBuffersInBacklogUnsafe(bufferConsumer.isBuffer());
                }

                // if we have an empty finished buffer and the exclusive credit is 0, we just return
                // the empty buffer so that the downstream task can release the allocated credit for
                // this empty buffer, this happens in two main scenarios currently:
                // 1. all data of a buffer builder has been read and after that the buffer builder
                // is finished
                // 2. in approximate recovery mode, a partial record takes a whole buffer builder
                //如果我们有一个空的已完成缓冲区，并且独占信用为0，
                //我们只是返回空缓冲区，以便下游任务可以释放为此空缓冲区分配的信用，
                // 目前在两种主要情况下会发生这种情况:
                // 1.buffer builder的所有数据已被读取，之后buffer builder完成
                // 2.在近似恢复模式下，部分记录将占用整个buffer builder
                if (receiverExclusiveBuffersPerChannel == 0 && bufferConsumer.isFinished()) {
                    break;
                }

                if (buffer.readableBytes() > 0) {
                    break;
                }
                //释放此缓冲区一次，即，如果引用计数达到0 ，则减少引用计数并回收缓冲区。
                buffer.recycleBuffer();
                buffer = null;
                if (!bufferConsumer.isFinished()) {
                    break;
                }
            }

            if (buffer == null) {
                return null;
            }

            if (buffer.getDataType().isBlockingUpstream()) {
                isBlocked = true;
            }

            updateStatistics(buffer);
            // Do not report last remaining buffer on buffers as available to read (assuming it's
            // unfinished).
            // It will be reported for reading either on flush or when the number of buffers in the
            // queue
            // will be 2 or more.
            //不要将缓冲区上最后剩余的缓冲区报告为可读取 (假设它未完成)。在刷新时或当队列中的缓冲区数量为2或更多时，将报告读取。
            NetworkActionsLogger.traceOutput(
                    "PipelinedSubpartition#pollBuffer",
                    buffer,
                    parent.getOwningTaskName(),
                    subpartitionInfo);
            return new BufferAndBacklog(
                    buffer,
                    getBuffersInBacklogUnsafe(),
                    isDataAvailableUnsafe() ? getNextBufferTypeUnsafe() : Buffer.DataType.NONE,
                    sequenceNumber++);
        }
    }

    @GuardedBy("buffers")
    private void completeTimeoutableCheckpointBarrier(BufferConsumer bufferConsumer) {
        CheckpointBarrier barrier = parseAndCheckTimeoutableCheckpointBarrier(bufferConsumer);
        if (!isChannelStateFutureAvailable(barrier.getId())) {
            // It happens on a previously aborted checkpoint.
            //它发生在先前中止的检查点上。
            return;
        }
        completeChannelStateFuture(Collections.emptyList(), null);
    }

    void resumeConsumption() {
        synchronized (buffers) {
            checkState(isBlocked, "Should be blocked by checkpoint.");

            isBlocked = false;
        }
    }

    public void acknowledgeAllDataProcessed() {
        parent.onSubpartitionAllDataProcessed(subpartitionInfo.getSubPartitionIdx());
    }

    @Override
    public boolean isReleased() {
        return isReleased;
    }

    @Override
    public PipelinedSubpartitionView createReadView(
            BufferAvailabilityListener availabilityListener) {
        synchronized (buffers) {
            checkState(!isReleased);
            checkState(
                    readView == null,
                    "Subpartition %s of is being (or already has been) consumed, "
                            + "but pipelined subpartitions can only be consumed once.",
                    getSubPartitionIndex(),
                    parent.getPartitionId());

            LOG.debug(
                    "{}: Creating read view for subpartition {} of partition {}.",
                    parent.getOwningTaskName(),
                    getSubPartitionIndex(),
                    parent.getPartitionId());

            //创建PipelinedSubpartitionView类型的读取视图
            readView = new PipelinedSubpartitionView(this, availabilityListener);
        }

        return readView;
    }

    public ResultSubpartitionView.AvailabilityWithBacklog getAvailabilityAndBacklog(
            boolean isCreditAvailable) {
        synchronized (buffers) {
            boolean isAvailable;
            if (isCreditAvailable) {
                isAvailable = isDataAvailableUnsafe();
            } else {
                isAvailable = getNextBufferTypeUnsafe().isEvent();
            }
            return new ResultSubpartitionView.AvailabilityWithBacklog(
                    isAvailable, getBuffersInBacklogUnsafe());
        }
    }

    @GuardedBy("buffers")
    private boolean isDataAvailableUnsafe() {
        assert Thread.holdsLock(buffers);

        return !isBlocked && (flushRequested || getNumberOfFinishedBuffers() > 0);
    }

    private Buffer.DataType getNextBufferTypeUnsafe() {
        assert Thread.holdsLock(buffers);

        final BufferConsumerWithPartialRecordLength first = buffers.peek();
        return first != null ? first.getBufferConsumer().getDataType() : Buffer.DataType.NONE;
    }

    // ------------------------------------------------------------------------

    @Override
    public int getNumberOfQueuedBuffers() {
        synchronized (buffers) {
            return buffers.size();
        }
    }

    @Override
    public void bufferSize(int desirableNewBufferSize) {
        if (desirableNewBufferSize < 0) {
            throw new IllegalArgumentException("New buffer size can not be less than zero");
        }
        synchronized (buffers) {
            bufferSize = desirableNewBufferSize;
        }
    }

    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        final long numBuffers;
        final long numBytes;
        final boolean finished;
        final boolean hasReadView;

        synchronized (buffers) {
            numBuffers = getTotalNumberOfBuffersUnsafe();
            numBytes = getTotalNumberOfBytesUnsafe();
            finished = isFinished;
            hasReadView = readView != null;
        }

        return String.format(
                "%s#%d [number of buffers: %d (%d bytes), number of buffers in backlog: %d, finished? %s, read view? %s]",
                this.getClass().getSimpleName(),
                getSubPartitionIndex(),
                numBuffers,
                numBytes,
                getBuffersInBacklogUnsafe(),
                finished,
                hasReadView);
    }

    @Override
    public int unsynchronizedGetNumberOfQueuedBuffers() {
        // since we do not synchronize, the size may actually be lower than 0!
        //由于我们不同步，因此大小实际上可能低于0!
        return Math.max(buffers.size(), 0);
    }

    @Override
    public void flush() {
        final boolean notifyDataAvailable;
        synchronized (buffers) {
            if (buffers.isEmpty() || flushRequested) {
                return;
            }
            // if there is more than 1 buffer, we already notified the reader
            // (at the latest when adding the second buffer)
            //如果有超过1个缓冲区，我们已经通知读者 (最迟在添加第二个缓冲区时)
            boolean isDataAvailableInUnfinishedBuffer =
                    buffers.size() == 1 && buffers.peek().getBufferConsumer().isDataAvailable();
            notifyDataAvailable = !isBlocked && isDataAvailableInUnfinishedBuffer;
            flushRequested = buffers.size() > 1 || isDataAvailableInUnfinishedBuffer;
        }
        if (notifyDataAvailable) {
            notifyDataAvailable();
        }
    }

    @Override
    protected long getTotalNumberOfBuffersUnsafe() {
        return totalNumberOfBuffers;
    }

    @Override
    protected long getTotalNumberOfBytesUnsafe() {
        return totalNumberOfBytes;
    }

    Throwable getFailureCause() {
        return parent.getFailureCause();
    }

    private void updateStatistics(BufferConsumer buffer) {
        totalNumberOfBuffers++;
    }

    private void updateStatistics(Buffer buffer) {
        totalNumberOfBytes += buffer.getSize();
    }

    @GuardedBy("buffers")
    private void decreaseBuffersInBacklogUnsafe(boolean isBuffer) {
        assert Thread.holdsLock(buffers);
        if (isBuffer) {
            buffersInBacklog--;
        }
    }

    /**
     * Increases the number of non-event buffers by one after adding a non-event buffer into this
     * subpartition.
     */
    //将非事件缓冲区添加到此子分区后，将非事件缓冲区的数量增加1。
    @GuardedBy("buffers")
    private void increaseBuffersInBacklog(BufferConsumer buffer) {
        assert Thread.holdsLock(buffers);

        if (buffer != null && buffer.isBuffer()) {
            buffersInBacklog++;
        }
    }

    /** Gets the number of non-event buffers in this subpartition. */
    //获取此子分区中非事件缓冲区的数量。
    @SuppressWarnings("FieldAccessNotGuarded")
    @Override
    public int getBuffersInBacklogUnsafe() {
        if (isBlocked || buffers.isEmpty()) {
            return 0;
        }

        if (flushRequested
                || isFinished
                || !checkNotNull(buffers.peekLast()).getBufferConsumer().isBuffer()) {
            return buffersInBacklog;
        } else {
            return Math.max(buffersInBacklog - 1, 0);
        }
    }

    @GuardedBy("buffers")
    private boolean shouldNotifyDataAvailable() {
        // Notify only when we added first finished buffer.
        // 仅当我们添加第一个完成的缓冲区时才通知。
        return readView != null
                && !flushRequested
                && !isBlocked
                && getNumberOfFinishedBuffers() == 1;
    }

    private void notifyDataAvailable() {
        final PipelinedSubpartitionView readView = this.readView;
        if (readView != null) {
            readView.notifyDataAvailable();
        }
    }

    private void notifyPriorityEvent(int prioritySequenceNumber) {
        final PipelinedSubpartitionView readView = this.readView;
        if (readView != null && prioritySequenceNumber != DEFAULT_PRIORITY_SEQUENCE_NUMBER) {
            readView.notifyPriorityEvent(prioritySequenceNumber);
        }
    }

    private int getNumberOfFinishedBuffers() {
        assert Thread.holdsLock(buffers);

        // NOTE: isFinished() is not guaranteed to provide the most up-to-date state here
        // worst-case: a single finished buffer sits around until the next flush() call
        // (but we do not offer stronger guarantees anyway)
        //注意： isFinished() 不能保证在最坏的情况下提供最新的状态：
        // 单个已完成的缓冲区会一直保留，直到下一次调用flush()为止（但我们无论如何也不提供更强的保证）
        final int numBuffers = buffers.size();
        if (numBuffers == 1 && buffers.peekLast().getBufferConsumer().isFinished()) {
            return 1;
        }

        // We assume that only last buffer is not finished.
        //我们假设只有最后一个缓冲区尚未完成。
        return Math.max(0, numBuffers - 1);
    }

    Buffer buildSliceBuffer(BufferConsumerWithPartialRecordLength buffer) {
        return buffer.build();
    }

    /** for testing only. */
    @VisibleForTesting
    BufferConsumerWithPartialRecordLength getNextBuffer() {
        return buffers.poll();
    }

    /** for testing only. */
    // suppress this warning as it is only for testing.
    @SuppressWarnings("FieldAccessNotGuarded")
    @VisibleForTesting
    CompletableFuture<List<Buffer>> getChannelStateFuture() {
        return channelStateFuture;
    }

    // suppress this warning as it is only for testing.
    @SuppressWarnings("FieldAccessNotGuarded")
    @VisibleForTesting
    public long getChannelStateCheckpointId() {
        return channelStateCheckpointId;
    }
}
