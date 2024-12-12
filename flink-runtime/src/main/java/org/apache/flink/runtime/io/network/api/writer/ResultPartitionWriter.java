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

package org.apache.flink.runtime.io.network.api.writer;

import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.AvailabilityProvider;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * A record-oriented runtime result writer API for producing results.
 *
 * <p>If {@link ResultPartitionWriter#close()} is called before {@link
 * ResultPartitionWriter#fail(Throwable)} or {@link ResultPartitionWriter#finish()}, it abruptly
 * triggers failure and cancellation of production. In this case {@link
 * ResultPartitionWriter#fail(Throwable)} still needs to be called afterwards to fully release all
 * resources associated the partition and propagate failure cause to the consumer if possible.
 */
//用于生成结果的面向记录的运行时结果编写器 API。
//如果在fail(Throwable)或finish()之前调用close() ，它会突然触发失败并取消生产。
//在这种情况下，之后仍然需要调用fail(Throwable)来完全释放与分区相关的所有资源，并在可能的情况下将失败原因传播给消费者。
public interface ResultPartitionWriter extends AutoCloseable, AvailabilityProvider {

    /** Setup partition, potentially heavy-weight, blocking operation comparing to just creation. */
    //设置分区，潜在的重量级，阻塞操作相比，只是创建。
    void setup() throws IOException;

    ResultPartitionID getPartitionId();

    int getNumberOfSubpartitions();

    int getNumTargetKeyGroups();

    /** Sets the max overdraft buffer size of per gate. */
    //设置每个门的最大透支缓冲区大小
    void setMaxOverdraftBuffersPerGate(int maxOverdraftBuffersPerGate);

    /** Writes the given serialized record to the target subpartition. */
    //将给定的序列化记录写入目标子分区。
    void emitRecord(ByteBuffer record, int targetSubpartition) throws IOException;

    /**
     * Writes the given serialized record to all subpartitions. One can also achieve the same effect
     * by emitting the same record to all subpartitions one by one, however, this method can have
     * better performance for the underlying implementation can do some optimizations, for example
     * coping the given serialized record only once to a shared channel which can be consumed by all
     * subpartitions.
     */
    //将给定的序列化记录写入所有子分区。
    //也可以通过将相同的记录一一发送到所有子分区来达到相同的效果，
    // 但是，这种方法可以具有更好的性能，因为底层实现可以做一些优化，
    //例如仅将给定的序列化记录复制到共享通道一次可以被所有子分区使用
    void broadcastRecord(ByteBuffer record) throws IOException;

    /** Writes the given {@link AbstractEvent} to all subpartitions. */
    //将给定的AbstractEvent写入所有子分区。
    void broadcastEvent(AbstractEvent event, boolean isPriorityEvent) throws IOException;

    /** Timeout the aligned barrier to unaligned barrier. */
    //将对齐的屏障超时到未对齐的屏障。
    void alignedBarrierTimeout(long checkpointId) throws IOException;

    /** Abort the checkpoint. */
    //中止检查点。
    void abortCheckpoint(long checkpointId, CheckpointException cause);

    /**
     * Notifies the downstream tasks that this {@code ResultPartitionWriter} have emitted all the
     * user records.
     *
     * @param mode tells if we should flush all records or not (it is false in case of
     *     stop-with-savepoint (--no-drain))
     */
    //通知下游任务此ResultPartitionWriter已发出所有用户记录
    void notifyEndOfData(StopMode mode) throws IOException;

    /**
     * Gets the future indicating whether all the records has been processed by the downstream
     * tasks.
     */
    //获取表示所有记录是否已被下游任务处理的 future。
    CompletableFuture<Void> getAllDataProcessedFuture();

    /** Sets the metric group for the {@link ResultPartitionWriter}. */
    //设置ResultPartitionWriter的指标组。
    void setMetricGroup(TaskIOMetricGroup metrics);

    /** Returns a reader for the subpartition with the given index range. */
    //返回具有给定索引范围的子分区的读取器。
    ResultSubpartitionView createSubpartitionView(
            ResultSubpartitionIndexSet indexSet, BufferAvailabilityListener availabilityListener)
            throws IOException;

    /** Manually trigger the consumption of data from all subpartitions. */
    //手动触发所有子分区的数据消耗。
    void flushAll();

    /** Manually trigger the consumption of data from the given subpartitions. */
    //手动触发给定子分区的数据消耗。
    void flush(int subpartitionIndex);

    /**
     * Fail the production of the partition.
     *
     * <p>This method propagates non-{@code null} failure causes to consumers on a best-effort
     * basis. This call also leads to the release of all resources associated with the partition.
     * Closing of the partition is still needed afterwards if it has not been done before.
     *
     * @param throwable failure cause
     */
    //分区制作失败。
    //此方法尽力将非null故障原因传播给消费者。
    //此调用还会导致与该分区关联的所有资源的释放。如果之前没有关闭分区，之后仍然需要关闭分区
    void fail(@Nullable Throwable throwable);

    /**
     * Successfully finish the production of the partition.
     *
     * <p>Closing of partition is still needed afterwards.
     */
    //顺利完成隔断的制作。
    //之后仍需要关闭分区。
    void finish() throws IOException;

    boolean isFinished();

    /**
     * Releases the partition writer which releases the produced data and no reader can consume the
     * partition any more.
     */
    //释放分区写入器，释放生成的数据，并且任何读取器都不能再使用该分区。
    void release(Throwable cause);

    boolean isReleased();

    /**
     * Closes the partition writer which releases the allocated resource, for example the buffer
     * pool.
     */
    //关闭分区写入器，释放分配的资源，例如缓冲池。
    void close() throws Exception;
}
