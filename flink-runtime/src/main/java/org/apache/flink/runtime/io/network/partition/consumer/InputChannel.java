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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.PartitionException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An input channel consumes a single {@link ResultSubpartitionView}.
 *
 * <p>For each channel, the consumption life cycle is as follows:
 *
 * <ol>
 *   <li>{@link #requestSubpartitions()}
 *   <li>{@link #getNextBuffer()}
 *   <li>{@link #releaseAllResources()}
 * </ol>
 */
//输入通道消耗单个ResultSubpartitionView 。
//对于每个渠道，消费生命周期如下：
//requestSubpartitions()
//getNextBuffer()
//releaseAllResources()
public abstract class InputChannel {
    /** The info of the input channel to identify it globally within a task. */
    //输入通道的信息，用于在任务中全局识别它。
    protected final InputChannelInfo channelInfo;

    /** The parent partition of the subpartitions consumed by this channel. */
    //该通道消耗的子分区的父分区。
    protected final ResultPartitionID partitionId;

    /** The indexes of the subpartitions consumed by this channel. */
    //该通道消耗的子分区的索引。
    protected final ResultSubpartitionIndexSet consumedSubpartitionIndexSet;

    protected final SingleInputGate inputGate;

    // - Asynchronous error notification --------------------------------------
    //异步错误通知
    private final AtomicReference<Throwable> cause = new AtomicReference<Throwable>();

    // - Partition request backoff --------------------------------------------
    //分区请求退避
    /** The initial backoff (in ms). */
    //初始退避（以毫秒为单位）。
    protected final int initialBackoff;

    /** The maximum backoff (in ms). */
    //最大退避（以毫秒为单位）。
    protected final int maxBackoff;

    protected final Counter numBytesIn;

    protected final Counter numBuffersIn;

    /**
     * The index of the subpartition if {@link #consumedSubpartitionIndexSet} contains only one
     * subpartition, or -1.
     */
    //如果consumedSubpartitionIndexSet仅包含一个子分区，则为子分区的索引，否则为 -1。
    private final int subpartitionId;

    /** The current backoff (in ms). */
    //当前的退避（以毫秒为单位）。
    protected int currentBackoff;

    protected InputChannel(
            SingleInputGate inputGate,
            int channelIndex,
            ResultPartitionID partitionId,
            ResultSubpartitionIndexSet consumedSubpartitionIndexSet,
            int initialBackoff,
            int maxBackoff,
            Counter numBytesIn,
            Counter numBuffersIn) {

        checkArgument(channelIndex >= 0);

        int initial = initialBackoff;
        int max = maxBackoff;

        checkArgument(initial >= 0 && initial <= max);

        this.inputGate = checkNotNull(inputGate);
        this.channelInfo = new InputChannelInfo(inputGate.getGateIndex(), channelIndex);
        this.partitionId = checkNotNull(partitionId);

        this.consumedSubpartitionIndexSet = consumedSubpartitionIndexSet;
        this.subpartitionId =
                consumedSubpartitionIndexSet.size() > 1
                        ? -1
                        : consumedSubpartitionIndexSet.values().iterator().next();

        this.initialBackoff = initial;
        this.maxBackoff = max;
        this.currentBackoff = 0;

        this.numBytesIn = numBytesIn;
        this.numBuffersIn = numBuffersIn;
    }

    // ------------------------------------------------------------------------
    // Properties
    // ------------------------------------------------------------------------

    /** Returns the index of this channel within its {@link SingleInputGate}. */
    //返回此通道在其SingleInputGate中的索引。
    public int getChannelIndex() {
        return channelInfo.getInputChannelIdx();
    }

    /**
     * Returns the info of this channel, which uniquely identifies the channel in respect to its
     * operator instance.
     */
    //返回此通道的信息，该信息根据其运算符实例唯一标识该通道。
    public InputChannelInfo getChannelInfo() {
        return channelInfo;
    }

    public ResultPartitionID getPartitionId() {
        return partitionId;
    }

    public ResultSubpartitionIndexSet getConsumedSubpartitionIndexSet() {
        return consumedSubpartitionIndexSet;
    }

    /**
     * After sending a {@link org.apache.flink.runtime.io.network.api.CheckpointBarrier} of
     * exactly-once mode, the upstream will be blocked and become unavailable. This method tries to
     * unblock the corresponding upstream and resume data consumption.
     */
    //发送exactly-once模式的CheckpointBarrier后，上游会被阻塞，变得不可用。该方法尝试解锁相应的上游并恢复数据消耗。
    public abstract void resumeConsumption() throws IOException;

    /**
     * When received {@link EndOfData} from one channel, it need to acknowledge after this event get
     * processed.
     */
    //当从一个通道接收到EndOfData时，需要在处理该事件后进行确认。
    public abstract void acknowledgeAllRecordsProcessed() throws IOException;

    /**
     * Notifies the owning {@link SingleInputGate} that this channel became non-empty.
     *
     * <p>This is guaranteed to be called only when a Buffer was added to a previously empty input
     * channel. The notion of empty is atomically consistent with the flag {@link
     * BufferAndAvailability#moreAvailable()} when polling the next buffer from this channel.
     *
     * <p><b>Note:</b> When the input channel observes an exception, this method is called
     * regardless of whether the channel was empty before. That ensures that the parent InputGate
     * will always be notified about the exception.
     */
    //通知拥有的SingleInputGate此通道变为非空。
    //这保证仅在将缓冲区添加到先前空的输入通道时才被调用。
    // 空的概念在原子上与inputchannel. bufferandavailability. modreavailability. modreavailability.
    // moreavailability. modreavailability. modreavailability. modreavailability. modreavailability.
    // modreavailability. modavailability. modiable () 时 () 标志一致，
    // 当轮询从这个通道轮询这个通道时，从这个通道轮询的概念是
    //注意: 输入通道观察到异常时，无论通道之前是否为空，都会调用此方法。这确保了父InputGate将始终收到有关异常的通知。
    protected void notifyChannelNonEmpty() {
        inputGate.notifyChannelNonEmpty(this);
    }

    public void notifyPriorityEvent(int priorityBufferNumber) {
        inputGate.notifyPriorityEvent(this, priorityBufferNumber);
    }

    protected void notifyBufferAvailable(int numAvailableBuffers) throws IOException {}

    // ------------------------------------------------------------------------
    // Consume
    // ------------------------------------------------------------------------

    /**
     * Requests the subpartitions specified by {@link #partitionId} and {@link
     * #consumedSubpartitionIndexSet}.
     */
    //请求指定的子分区partitionId和consumedSubpartitionIndexSet。
    abstract void requestSubpartitions() throws IOException, InterruptedException;

    /**
     * Returns the index of the subpartition where the next buffer locates, or -1 if there is no
     * buffer available and the subpartition to be consumed is not determined.
     */
    //返回下一个缓冲区所在子分区的索引，如果没有可用缓冲区且未确定要消费的子分区，则返回-1。
    public int peekNextBufferSubpartitionId() throws IOException {
        if (subpartitionId >= 0) {
            return subpartitionId;
        }
        return peekNextBufferSubpartitionIdInternal();
    }

    /**
     * Returns the index of the subpartition where the next buffer locates, or -1 if there is no
     * buffer available and the subpartition to be consumed is not determined.
     */
    //返回下一个缓冲区所在子分区的索引，如果没有可用缓冲区且未确定要消费的子分区，则返回-1。
    protected abstract int peekNextBufferSubpartitionIdInternal() throws IOException;

    /**
     * Returns the next buffer from the consumed subpartitions or {@code Optional.empty()} if there
     * is no data to return.
     */
    //如果没有数据可返回，则返回已使用的子分区中的下一个缓冲区或Optional. empty() 。
    public abstract Optional<BufferAndAvailability> getNextBuffer()
            throws IOException, InterruptedException;

    /**
     * Called by task thread when checkpointing is started (e.g., any input channel received
     * barrier).
     */
    //当检查点启动时由任务线程调用（例如，任何输入通道接收屏障）。
    public void checkpointStarted(CheckpointBarrier barrier) throws CheckpointException {}

    /** Called by task thread on cancel/complete to clean-up temporary data. */
    //由任务线程在取消/ 完成时调用以清理临时数据。
    public void checkpointStopped(long checkpointId) {}

    public void convertToPriorityEvent(int sequenceNumber) throws IOException {}

    // ------------------------------------------------------------------------
    // Task events
    // ------------------------------------------------------------------------

    /**
     * Sends a {@link TaskEvent} back to the task producing the consumed result partition.
     *
     * <p><strong>Important</strong>: The producing task has to be running to receive backwards
     * events. This means that the result type needs to be pipelined and the task logic has to
     * ensure that the producer will wait for all backwards events. Otherwise, this will lead to an
     * Exception at runtime.
     */
    //将TaskEvent发送回生成消耗结果分区的任务。
    //重要提示：生产任务必须运行才能接收向后事件。
    // 这意味着结果类型需要管道化，并且任务逻辑必须确保生产者将等待所有向后事件。否则，这将导致运行时出现异常。
    abstract void sendTaskEvent(TaskEvent event) throws IOException;

    // ------------------------------------------------------------------------
    // Life cycle
    // ------------------------------------------------------------------------

    abstract boolean isReleased();

    /** Releases all resources of the channel. */
    //释放频道所有资源。
    abstract void releaseAllResources() throws IOException;

    abstract void announceBufferSize(int newBufferSize);

    abstract int getBuffersInUseCount();

    // ------------------------------------------------------------------------
    // Error notification
    // ------------------------------------------------------------------------

    /**
     * Checks for an error and rethrows it if one was reported.
     *
     * <p>Note: Any {@link PartitionException} instances should not be transformed and make sure
     * they are always visible in task failure cause.
     */
    //检查是否有错误，如果报告错误则重新抛出错误。
    //注意：不应转换任何PartitionException实例，并确保它们在任务失败原因中始终可见。
    protected void checkError() throws IOException {
        final Throwable t = cause.get();

        if (t != null) {
            if (t instanceof CancelTaskException) {
                throw (CancelTaskException) t;
            }
            if (t instanceof IOException) {
                throw (IOException) t;
            } else {
                throw new IOException(t);
            }
        }
    }

    /**
     * Atomically sets an error for this channel and notifies the input gate about available data to
     * trigger querying this channel by the task thread.
     */
    //原子地为此通道设置错误，并通知inputGate关可用数据的信息，以触发任务线程查询该通道。
    protected void setError(Throwable cause) {
        if (this.cause.compareAndSet(null, checkNotNull(cause))) {
            // Notify the input gate.
            notifyChannelNonEmpty();
        }
    }

    // ------------------------------------------------------------------------
    // Partition request exponential backoff
    // ------------------------------------------------------------------------

    /** Returns the current backoff in ms. */
    //返回当前退避时间（以毫秒为单位）。
    protected int getCurrentBackoff() {
        return currentBackoff <= 0 ? 0 : currentBackoff;
    }

    /**
     * Increases the current backoff and returns whether the operation was successful.
     *
     * @return <code>true</code>, iff the operation was successful. Otherwise, <code>false</code>.
     */
    //增加当前退避并返回操作是否成功。
    //返回：
    //true ，当且仅当操作成功。否则为false 。
    protected boolean increaseBackoff() {
        // Backoff is disabled
        //退避已禁用
        if (initialBackoff == 0) {
            return false;
        }

        if (currentBackoff == 0) {
            // This is the first time backing off
            //这是第一次后退
            currentBackoff = initialBackoff;

            return true;
        }

        // Continue backing off
        //继续后退
        else if (currentBackoff < maxBackoff) {
            currentBackoff = Math.min(currentBackoff * 2, maxBackoff);

            return true;
        }

        // Reached maximum backoff
        // 达到最大退避
        return false;
    }

    // ------------------------------------------------------------------------
    // Metric related method
    // ------------------------------------------------------------------------

    public int unsynchronizedGetNumberOfQueuedBuffers() {
        return 0;
    }

    public long unsynchronizedGetSizeOfQueuedBuffers() {
        return 0;
    }

    /**
     * Notify the upstream the id of required segment that should be sent to netty connection.
     *
     * @param subpartitionId The id of the corresponding subpartition.
     * @param segmentId The id of required segment.
     */
    //通知上游应发送到 netty 连接的所需段的 id。
    //参数：
    //subpartitionId – 相应子分区的 id。
    //segmentId – 所需段的 ID。
    public void notifyRequiredSegmentId(int subpartitionId, int segmentId) throws IOException {}

    // ------------------------------------------------------------------------

    /**
     * A combination of a {@link Buffer} and a flag indicating availability of further buffers, and
     * the backlog length indicating how many non-event buffers are available in the subpartitions.
     */
    //Buffer和指示其他缓冲区可用性的标志的组合，以及指示子分区中有多少个非事件缓冲区可用的积压长度。
    public static final class BufferAndAvailability {

        private final Buffer buffer;
        private final Buffer.DataType nextDataType;
        private final int buffersInBacklog;
        private final int sequenceNumber;

        public BufferAndAvailability(
                Buffer buffer,
                Buffer.DataType nextDataType,
                int buffersInBacklog,
                int sequenceNumber) {
            this.buffer = checkNotNull(buffer);
            this.nextDataType = checkNotNull(nextDataType);
            this.buffersInBacklog = buffersInBacklog;
            this.sequenceNumber = sequenceNumber;
        }

        public Buffer buffer() {
            return buffer;
        }

        public boolean moreAvailable() {
            return nextDataType != Buffer.DataType.NONE;
        }

        public boolean morePriorityEvents() {
            return nextDataType.hasPriority();
        }

        public int buffersInBacklog() {
            return buffersInBacklog;
        }

        public boolean hasPriority() {
            return buffer.getDataType().hasPriority();
        }

        public int getSequenceNumber() {
            return sequenceNumber;
        }

        @Override
        public String toString() {
            return "BufferAndAvailability{"
                    + "buffer="
                    + buffer
                    + ", nextDataType="
                    + nextDataType
                    + ", buffersInBacklog="
                    + buffersInBacklog
                    + ", sequenceNumber="
                    + sequenceNumber
                    + '}';
        }
    }

    void setup() throws IOException {}
}
