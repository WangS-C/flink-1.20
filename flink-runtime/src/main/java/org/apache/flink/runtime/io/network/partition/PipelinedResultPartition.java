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

import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.ResultSubpartitionInfo;
import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.logger.NetworkActionsLogger;
import org.apache.flink.runtime.io.network.partition.consumer.EndOfChannelStateEvent;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A result output of a task, pipelined (streamed) to the receivers.
 *
 * <p>This result partition implementation is used both in batch and streaming. For streaming, it
 * supports low latency transfers (ensure data is sent within x milliseconds) or unconstrained while
 * for batch it transfers only once a buffer is full. Additionally, for streaming use this typically
 * limits the length of the buffer backlog to not have too much data in flight, while for batch we
 * do not constrain this.
 *
 * <h2>Specifics of the PipelinedResultPartition</h2>
 *
 * <p>The PipelinedResultPartition cannot reconnect once a consumer disconnects (finished or
 * errored). Once all consumers have disconnected (released the subpartition, notified via the call
 * {@link #onConsumedSubpartition(int)}) then the partition as a whole is disposed and all buffers
 * are freed.
 */
//任务的结果输出，通过管道（流式传输）到接收器。
//此结果分区实现可用于批处理和流处理。对于流式传输，它支持低延迟传输（确保数据在 x 毫秒内发送）或不受限制，
// 而对于批处理，它仅在缓冲区已满时才传输。
//此外，对于流式使用，这通常会限制缓冲区积压的长度，以免传输中的数据过多，而对于批处理，我们不限制这一点。
//PipelinedResultPartition 的细节
//一旦使用者断开连接（完成或出错），PipelinedResultPartition 就无法重新连接。
//一旦所有消费者断开连接（释放子分区，通过调用onConsumedSubpartition(int)通知），则整个分区将被释放，所有缓冲区将被释放。
public class PipelinedResultPartition extends BufferWritingResultPartition
        implements CheckpointedResultPartition, ChannelStateHolder {
    private static final int PIPELINED_RESULT_PARTITION_ITSELF = -42;

    /**
     * The lock that guard operations which can be asynchronously propagated from the networks
     * threads.
     */
    //保护可以从网络线程异步传播的操作的锁。
    private final Object lock = new Object();

    /**
     * A flag for each subpartition indicating whether the downstream task has processed all the
     * user records.
     */
    @GuardedBy("lock")
    private final boolean[] allRecordsProcessedSubpartitions;

    /**
     * The total number of subpartitions whose user records have not been fully processed by the
     * downstream tasks yet.
     */
    //用户记录尚未被下游任务完全处理的子分区总数。
    @GuardedBy("lock")
    private int numNotAllRecordsProcessedSubpartitions;

    @GuardedBy("lock")
    private boolean hasNotifiedEndOfUserRecords;

    /**
     * The future represents whether all the records has been processed by all the downstream tasks.
     */
    //future代表所有下游任务是否已经处理了所有记录。
    @GuardedBy("lock")
    private final CompletableFuture<Void> allRecordsProcessedFuture = new CompletableFuture<>();

    /**
     * A flag for each subpartition indicating whether it was already consumed or not, to make
     * releases idempotent.
     */
    //每个子分区的标志，指示它是否已被消耗，以使发布幂等。
    @GuardedBy("lock")
    private final boolean[] consumedSubpartitions;

    /**
     * The total number of references to subpartitions of this result. The result partition can be
     * safely released, iff the reference count is zero. Every subpartition is a user of the result
     * as well the {@link PipelinedResultPartition} is a user itself, as it's writing to those
     * results. Even if all consumers are released, partition can not be released until writer
     * releases the partition as well.
     */
    //对此结果的子分区的引用总数。如果引用计数为零，则可以安全地释放结果分区。
    //每个子分区都是结果的用户， PipelinedResultPartition本身也是用户，因为它正在写入这些结果。
    //即使所有消费者都被释放，分区也无法被释放，直到 writer 也释放该分区。
    @GuardedBy("lock")
    private int numberOfUsers;

    public PipelinedResultPartition(
            String owningTaskName,
            int partitionIndex,
            ResultPartitionID partitionId,
            ResultPartitionType partitionType,
            ResultSubpartition[] subpartitions,
            int numTargetKeyGroups,
            ResultPartitionManager partitionManager,
            @Nullable BufferCompressor bufferCompressor,
            SupplierWithException<BufferPool, IOException> bufferPoolFactory) {

        super(
                owningTaskName,
                partitionIndex,
                partitionId,
                checkResultPartitionType(partitionType),
                subpartitions,
                numTargetKeyGroups,
                partitionManager,
                bufferCompressor,
                bufferPoolFactory);

        this.allRecordsProcessedSubpartitions = new boolean[subpartitions.length];
        this.numNotAllRecordsProcessedSubpartitions = subpartitions.length;

        this.consumedSubpartitions = new boolean[subpartitions.length];
        this.numberOfUsers = subpartitions.length + 1;
    }

    @Override
    public void setChannelStateWriter(ChannelStateWriter channelStateWriter) {
        for (final ResultSubpartition subpartition : subpartitions) {
            if (subpartition instanceof ChannelStateHolder) {
                ((PipelinedSubpartition) subpartition).setChannelStateWriter(channelStateWriter);
            }
        }
    }

    /**
     * The pipelined partition releases automatically once all subpartition readers are released.
     * That is because pipelined partitions cannot be consumed multiple times, or reconnect.
     */
    //一旦所有子分区读取器被释放，管道分区就会自动释放。这是因为管道分区不能被多次使用或重新连接
    @Override
    void onConsumedSubpartition(int subpartitionIndex) {
        decrementNumberOfUsers(subpartitionIndex);
    }

    private void decrementNumberOfUsers(int subpartitionIndex) {
        if (isReleased()) {
            return;
        }

        final int remainingUnconsumed;

        // we synchronize only the bookkeeping section, to avoid holding the lock during any
        // calls into other components
        //我们仅同步bookkeeping部分，以避免在调用其他组件期间持有锁
        synchronized (lock) {
            if (subpartitionIndex != PIPELINED_RESULT_PARTITION_ITSELF) {
                if (consumedSubpartitions[subpartitionIndex]) {
                    // repeated call - ignore
                    return;
                }

                consumedSubpartitions[subpartitionIndex] = true;
            }
            remainingUnconsumed = (--numberOfUsers);
        }

        LOG.debug(
                "{}: Received consumed notification for subpartition {}.", this, subpartitionIndex);

        if (remainingUnconsumed == 0) {
            partitionManager.onConsumedPartition(this);
        } else if (remainingUnconsumed < 0) {
            throw new IllegalStateException(
                    "Received consume notification even though all subpartitions are already consumed.");
        }
    }

    @Override
    public ResultSubpartitionInfo getCheckpointedSubpartitionInfo(int subpartitionIndex) {
        return subpartitions[subpartitionIndex].getSubpartitionInfo();
    }

    @Override
    public void flushAll() {
        flushAllSubpartitions(false);
    }

    @Override
    public void flush(int targetSubpartition) {
        flushSubpartition(targetSubpartition, false);
    }

    @Override
    public void notifyEndOfData(StopMode mode) throws IOException {
        synchronized (lock) {
            if (!hasNotifiedEndOfUserRecords) {
                broadcastEvent(new EndOfData(mode), false);
                hasNotifiedEndOfUserRecords = true;
            }
        }
    }

    @Override
    public CompletableFuture<Void> getAllDataProcessedFuture() {
        return allRecordsProcessedFuture;
    }

    @Override
    public void onSubpartitionAllDataProcessed(int subpartition) {
        synchronized (lock) {
            if (allRecordsProcessedSubpartitions[subpartition]) {
                return;
            }

            allRecordsProcessedSubpartitions[subpartition] = true;
            numNotAllRecordsProcessedSubpartitions--;

            if (numNotAllRecordsProcessedSubpartitions == 0) {
                allRecordsProcessedFuture.complete(null);
            }
        }
    }

    @Override
    @SuppressWarnings("FieldAccessNotGuarded")
    public String toString() {
        return "PipelinedResultPartition "
                + partitionId.toString()
                + " ["
                + partitionType
                + ", "
                + subpartitions.length
                + " subpartitions, "
                + numberOfUsers
                + " pending consumptions]";
    }

    // ------------------------------------------------------------------------
    //   miscellaneous utils
    // ------------------------------------------------------------------------

    private static ResultPartitionType checkResultPartitionType(ResultPartitionType type) {
        checkArgument(
                type == ResultPartitionType.PIPELINED
                        || type == ResultPartitionType.PIPELINED_BOUNDED
                        || type == ResultPartitionType.PIPELINED_APPROXIMATE);
        return type;
    }

    @Override
    public void finishReadRecoveredState(boolean notifyAndBlockOnCompletion) throws IOException {
        if (!notifyAndBlockOnCompletion) {
            return;
        }
        try (BufferConsumer eventBufferConsumer =
                EventSerializer.toBufferConsumer(EndOfChannelStateEvent.INSTANCE, false)) {
            for (int i = 0; i < subpartitions.length; i++) {
                if (((PipelinedSubpartition) subpartitions[i]).isSupportChannelStateRecover()) {
                    addToSubpartition(i, eventBufferConsumer.copy(), 0);
                }
            }
        }
    }

    @Override
    public BufferBuilder requestBufferBuilderBlocking()
            throws IOException, RuntimeException, InterruptedException {
        return getBufferPool().requestBufferBuilderBlocking();
    }

    @Override
    public void addRecovered(int subpartitionIndex, BufferConsumer bufferConsumer)
            throws IOException {
        ResultSubpartition subpartition = subpartitions[subpartitionIndex];
        NetworkActionsLogger.traceRecover(
                "PipelinedSubpartition#addRecovered",
                bufferConsumer,
                getOwningTaskName(),
                subpartition.subpartitionInfo);

        if (addToSubpartition(subpartitionIndex, bufferConsumer, Integer.MIN_VALUE)
                == ResultSubpartition.ADD_BUFFER_ERROR_CODE) {
            throw new IOException("Buffer consumer couldn't be added to ResultSubpartition");
        }
    }

    @Override
    public void close() {
        decrementNumberOfUsers(PIPELINED_RESULT_PARTITION_ITSELF);
        super.close();
    }
}
