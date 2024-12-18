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
import org.apache.flink.runtime.checkpoint.channel.ResultSubpartitionInfo;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A single subpartition of a {@link ResultPartition} instance. */
//ResultPartition实例的单个子分区。
public abstract class ResultSubpartition {

    // The error code when adding a buffer fails.
    //添加缓冲区失败时的错误码。
    public static final int ADD_BUFFER_ERROR_CODE = -1;

    /** The info of the subpartition to identify it globally within a task. */
    //用于在任务中全局识别子分区的信息。
    protected final ResultSubpartitionInfo subpartitionInfo;

    /** The parent partition this subpartition belongs to. */
    //该子分区所属的父分区。
    protected final ResultPartition parent;

    // - Statistics ----------------------------------------------------------

    public ResultSubpartition(int index, ResultPartition parent) {
        this.parent = parent;
        this.subpartitionInfo = new ResultSubpartitionInfo(parent.getPartitionIndex(), index);
    }

    public ResultSubpartitionInfo getSubpartitionInfo() {
        return subpartitionInfo;
    }

    /** Gets the total numbers of buffers (data buffers plus events). */
    //获取缓冲区总数（数据缓冲区加上事件）。
    protected abstract long getTotalNumberOfBuffersUnsafe();

    protected abstract long getTotalNumberOfBytesUnsafe();

    public int getSubPartitionIndex() {
        return subpartitionInfo.getSubPartitionIdx();
    }

    /** Notifies the parent partition about a consumed {@link ResultSubpartitionView}. */
    //通知父分区有关已使用的ResultSubpartitionView的信息。
    protected void onConsumedSubpartition() {
        parent.onConsumedSubpartition(getSubPartitionIndex());
    }

    public abstract void alignedBarrierTimeout(long checkpointId) throws IOException;

    public abstract void abortCheckpoint(long checkpointId, CheckpointException cause);

    @VisibleForTesting
    public final int add(BufferConsumer bufferConsumer) throws IOException {
        return add(bufferConsumer, 0);
    }

    /**
     * Adds the given buffer.
     *
     * <p>The request may be executed synchronously, or asynchronously, depending on the
     * implementation.
     *
     * <p><strong>IMPORTANT:</strong> Before adding new {@link BufferConsumer} previously added must
     * be in finished state. Because of the performance reasons, this is only enforced during the
     * data reading. Priority events can be added while the previous buffer consumer is still open,
     * in which case the open buffer consumer is overtaken.
     *
     * @param bufferConsumer the buffer to add (transferring ownership to this writer)
     * @param partialRecordLength the length of bytes to skip in order to start with a complete
     *     record, from position index 0 of the underlying {@cite MemorySegment}.
     * @return the preferable buffer size for this subpartition or {@link #ADD_BUFFER_ERROR_CODE} if
     *     the add operation fails.
     * @throws IOException thrown in case of errors while adding the buffer
     */
    //添加给定的缓冲区。
    //请求可以同步执行，也可以异步执行，具体取决于实现方式。
    //重要提示: 添加之前添加的新BufferConsumer必须处于已完成状态。
    //由于性能原因，这仅在数据读取期间实施。
    //可以在前一个缓冲区消费者仍然打开时添加优先级事件，在这种情况下，打开的缓冲区消费者被超越。
    //参数:
    //bufferConsumer -要添加的缓冲区 (将所有权转移给此编写器)
    //partialRecordLength -要跳过的字节长度，以便从基础 @ cite MemorySegment的位置索引0开始完整记录。
    public abstract int add(BufferConsumer bufferConsumer, int partialRecordLength)
            throws IOException;

    public abstract void flush();

    /**
     * Writing of data is finished.
     *
     * @return the size of data written for this subpartition inside of finish.
     */
    //数据写入完成。
    //返回：
    //完成后为此子分区写入的数据大小。
    public abstract int finish() throws IOException;

    public abstract void release() throws IOException;

    public abstract ResultSubpartitionView createReadView(
            BufferAvailabilityListener availabilityListener) throws IOException;

    public abstract boolean isReleased();

    /** Gets the number of non-event buffers in this subpartition. */
    //获取此子分区中非事件缓冲区的数量。
    abstract int getBuffersInBacklogUnsafe();

    /**
     * Makes a best effort to get the current size of the queue. This method must not acquire locks
     * or interfere with the task and network threads in any way.
     */
    //尽最大努力获取队列的当前大小。此方法不得获取锁或以任何方式干扰任务和网络线程。
    public abstract int unsynchronizedGetNumberOfQueuedBuffers();

    /** Get the current size of the queue. */
    //获取队列的当前大小。
    public abstract int getNumberOfQueuedBuffers();

    public abstract void bufferSize(int desirableNewBufferSize);

    // ------------------------------------------------------------------------

    /**
     * A combination of a {@link Buffer} and the backlog length indicating how many non-event
     * buffers are available in the subpartition.
     */
    //Buffer和积压长度的组合，指示子分区中有多少个非事件缓冲区可用。
    public static final class BufferAndBacklog {
        private final Buffer buffer;
        private final int buffersInBacklog;
        private final Buffer.DataType nextDataType;
        private final int sequenceNumber;

        public BufferAndBacklog(
                Buffer buffer,
                int buffersInBacklog,
                Buffer.DataType nextDataType,
                int sequenceNumber) {
            this.buffer = checkNotNull(buffer);
            this.buffersInBacklog = buffersInBacklog;
            this.nextDataType = checkNotNull(nextDataType);
            this.sequenceNumber = sequenceNumber;
        }

        public Buffer buffer() {
            return buffer;
        }

        public boolean isDataAvailable() {
            return nextDataType != Buffer.DataType.NONE;
        }

        public int buffersInBacklog() {
            return buffersInBacklog;
        }

        public boolean isEventAvailable() {
            return nextDataType.isEvent();
        }

        public Buffer.DataType getNextDataType() {
            return nextDataType;
        }

        public int getSequenceNumber() {
            return sequenceNumber;
        }

        public static BufferAndBacklog fromBufferAndLookahead(
                Buffer current, Buffer.DataType nextDataType, int backlog, int sequenceNumber) {
            return new BufferAndBacklog(current, backlog, nextDataType, sequenceNumber);
        }
    }
}
