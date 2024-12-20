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

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkArgument;

/** A view to consume a {@link ResultSubpartition} instance. */
public interface ResultSubpartitionView {

    /**
     * Returns the next {@link Buffer} instance of this queue iterator.
     *
     * <p>If there is currently no instance available, it will return <code>null</code>. This might
     * happen for example when a pipelined queue producer is slower than the consumer or a spilled
     * queue needs to read in more data.
     *
     * <p><strong>Important</strong>: The consumer has to make sure that each buffer instance will
     * eventually be recycled with {@link Buffer#recycleBuffer()} after it has been consumed.
     */
    //返回此队列迭代器的下一个缓冲区实例。
    //如果当前没有可用的实例，它将返回null。例如，当流水线队列生产者比消费者慢或溢出的队列需要读入更多数据时，可能会发生这种情况。
    //重要: 使用者必须确保每个缓冲区实例在使用后最终都将使用buffer. recycleBuffer() 进行回收。
    @Nullable
    BufferAndBacklog getNextBuffer() throws IOException;

    void notifyDataAvailable();

    default void notifyPriorityEvent(int priorityBufferNumber) {}

    void releaseAllResources() throws IOException;

    boolean isReleased();

    void resumeConsumption();

    void acknowledgeAllDataProcessed();

    /**
     * {@link ResultSubpartitionView} can decide whether the failure cause should be reported to
     * consumer as failure (primary failure) or {@link ProducerFailedException} (secondary failure).
     * Secondary failure can be reported only if producer (upstream task) is guaranteed to failover.
     *
     * <p><strong>BEWARE:</strong> Incorrectly reporting failure cause as primary failure, can hide
     * the root cause of the failure from the user.
     */
    Throwable getFailureCause();

    /**
     * Get the availability and backlog of the view. The availability represents if the view is
     * ready to get buffer from it. The backlog represents the number of available data buffers.
     *
     * @param isCreditAvailable the availability of credits for this {@link ResultSubpartitionView}.
     * @return availability and backlog.
     */
    AvailabilityWithBacklog getAvailabilityAndBacklog(boolean isCreditAvailable);

    int unsynchronizedGetNumberOfQueuedBuffers();

    int getNumberOfQueuedBuffers();

    void notifyNewBufferSize(int newBufferSize);

    /**
     * In tiered storage shuffle mode, only required segments will be sent to prevent the redundant
     * buffer usage. Downstream will notify the upstream by this method to send required segments.
     *
     * @param subpartitionId The id of the corresponding subpartition.
     * @param segmentId The id of required segment.
     */
    default void notifyRequiredSegmentId(int subpartitionId, int segmentId) {}

    /**
     * Returns the index of the subpartition where the next buffer locates, or -1 if there is no
     * buffer available and the subpartition to be consumed is not determined.
     */
    int peekNextBufferSubpartitionId() throws IOException;

    /**
     * Availability of the {@link ResultSubpartitionView} and the backlog in the corresponding
     * {@link ResultSubpartition}.
     */
    class AvailabilityWithBacklog {

        private final boolean isAvailable;

        private final int backlog;

        public AvailabilityWithBacklog(boolean isAvailable, int backlog) {
            checkArgument(backlog >= 0, "Backlog must be non-negative.");

            this.isAvailable = isAvailable;
            this.backlog = backlog;
        }

        public boolean isAvailable() {
            return isAvailable;
        }

        public int getBacklog() {
            return backlog;
        }
    }
}
