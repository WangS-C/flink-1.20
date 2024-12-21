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
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.metrics.ResultPartitionBytesCounter;
import org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.taskexecutor.TaskExecutor;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.SupplierWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A result partition for data produced by a single task.
 *
 * <p>This class is the runtime part of a logical {@link IntermediateResultPartition}. Essentially,
 * a result partition is a collection of {@link Buffer} instances. The buffers are organized in one
 * or more {@link ResultSubpartition} instances or in a joint structure which further partition the
 * data depending on the number of consuming tasks and the data {@link DistributionPattern}.
 *
 * <p>Tasks, which consume a result partition have to request one of its subpartitions. The request
 * happens either remotely (see {@link RemoteInputChannel}) or locally (see {@link
 * LocalInputChannel})
 *
 * <h2>Life-cycle</h2>
 *
 * <p>The life-cycle of each result partition has three (possibly overlapping) phases:
 *
 * <ol>
 *   <li><strong>Produce</strong>:
 *   <li><strong>Consume</strong>:
 *   <li><strong>Release</strong>:
 * </ol>
 *
 * <h2>Buffer management</h2>
 *
 * <h2>State management</h2>
 */
//由单个任务生成的数据的结果分区。
//这个类是一个逻辑的IntermediateResultPartition的运行时部分。本质上，结果分区是缓冲区实例的集合。缓冲区被组织在一个或多个ResultSubpartition实例中或在联合结构中，该联合结构根据消耗任务的数量和数据分布模式进一步划分数据。
//使用结果分区的任务必须请求其子分区之一。请求发生在远程 (请参见RemoteInputChannel) 或本地 (请参见LocalInputChannel)
//生命周期
//每个结果分区的生命周期有三个 (可能重叠) 阶段:
//生产:
//消耗:
//释放:
//缓冲区管理
//状态管理
public abstract class ResultPartition implements ResultPartitionWriter {

    protected static final Logger LOG = LoggerFactory.getLogger(ResultPartition.class);

    private final String owningTaskName;

    //算子实例Task对应的结果分区下标
    private final int partitionIndex;

    protected final ResultPartitionID partitionId;

    /** Type of this partition. Defines the concrete subpartition implementation to use. */
    //此分区的类型。定义要使用的具体subpartition实现
    //结果分区类型。Flink流式应用类型一般是PIPELINED、PIPELINED_BOUNDED，
    //PIPELINED_BOUNDED代表数据写入速度有限制，当流数据出现背压时，任务不会在自己的Buffer里缓存大量的数据。
    protected final ResultPartitionType partitionType;

    protected final ResultPartitionManager partitionManager;

    //结果分区包含的结果子分区个数。
    protected final int numSubpartitions;

    private final int numTargetKeyGroups;

    // - Runtime state --------------------------------------------------------

    private final AtomicBoolean isReleased = new AtomicBoolean();

    //结果分区包含的缓冲池，用来存储分配到子分区的数据，类型是LocalBuffPool。
    //ResultPartition写数据时，会向bufferPool申请buffer并写入数据。
    protected BufferPool bufferPool;

    private boolean isFinished;

    private volatile Throwable cause;

    //缓冲池工厂类，用来创建bufferPool，类型是NetworkBufferPool。
    private final SupplierWithException<BufferPool, IOException> bufferPoolFactory;

    /** Used to compress buffer to reduce IO. */
    //用于压缩buffer以减少IO。
    @Nullable protected final BufferCompressor bufferCompressor;

    protected Counter numBytesOut = new SimpleCounter();

    protected Counter numBuffersOut = new SimpleCounter();

    protected ResultPartitionBytesCounter resultPartitionBytes;

    private boolean isNumberOfPartitionConsumerUndefined = false;

    public ResultPartition(
            String owningTaskName,
            int partitionIndex,
            ResultPartitionID partitionId,
            ResultPartitionType partitionType,
            int numSubpartitions,
            int numTargetKeyGroups,
            ResultPartitionManager partitionManager,
            @Nullable BufferCompressor bufferCompressor,
            SupplierWithException<BufferPool, IOException> bufferPoolFactory) {

        this.owningTaskName = checkNotNull(owningTaskName);
        Preconditions.checkArgument(0 <= partitionIndex, "The partition index must be positive.");
        this.partitionIndex = partitionIndex;
        this.partitionId = checkNotNull(partitionId);
        this.partitionType = checkNotNull(partitionType);
        this.numSubpartitions = numSubpartitions;
        this.numTargetKeyGroups = numTargetKeyGroups;
        this.partitionManager = checkNotNull(partitionManager);
        this.bufferCompressor = bufferCompressor;
        this.bufferPoolFactory = bufferPoolFactory;
        this.resultPartitionBytes = new ResultPartitionBytesCounter(numSubpartitions);
    }

    /**
     * Registers a buffer pool with this result partition.
     *
     * <p>There is one pool for each result partition, which is shared by all its sub partitions.
     *
     * <p>The pool is registered with the partition *after* it as been constructed in order to
     * conform to the life-cycle of task registrations in the {@link TaskExecutor}.
     */
    //向此结果分区注册缓冲池。
    //每个结果分区都有一个池，该池由其所有子分区共享。
    //为了符合TaskExecutor中任务注册的生命周期，池在构建后 * 在分区中注册。
    @Override
    public void setup() throws IOException {
        checkState(
                this.bufferPool == null,
                "Bug in result partition setup logic: Already registered buffer pool.");

        //创建bufferPool
        //调用的是org.apache.flink.runtime.io.network.partition.ResultPartitionFactory.createBufferPoolFactory
        this.bufferPool = checkNotNull(bufferPoolFactory.get());
        //做子类自己的设置操作
        setupInternal();
        //注册结果分区
        partitionManager.registerResultPartition(this);
    }

    /** Do the subclass's own setup operation. */
    //做子类自己的设置操作。
    protected abstract void setupInternal() throws IOException;

    public String getOwningTaskName() {
        return owningTaskName;
    }

    @Override
    public ResultPartitionID getPartitionId() {
        return partitionId;
    }

    public int getPartitionIndex() {
        return partitionIndex;
    }

    @Override
    public int getNumberOfSubpartitions() {
        return numSubpartitions;
    }

    public BufferPool getBufferPool() {
        return bufferPool;
    }

    public void isNumberOfPartitionConsumerUndefined(boolean isNumberOfPartitionConsumerUndefined) {
        this.isNumberOfPartitionConsumerUndefined = isNumberOfPartitionConsumerUndefined;
    }

    public boolean isNumberOfPartitionConsumerUndefined() {
        return isNumberOfPartitionConsumerUndefined;
    }

    /** Returns the total number of queued buffers of all subpartitions. */
    //返回所有子分区的排队缓冲区总数。
    public abstract int getNumberOfQueuedBuffers();

    /** Returns the total size in bytes of queued buffers of all subpartitions. */
    //返回所有子分区的排队缓冲区的总大小（以字节为单位）。
    public abstract long getSizeOfQueuedBuffersUnsafe();

    /** Returns the number of queued buffers of the given target subpartition. */
    //返回给定目标子分区的排队缓冲区数。
    public abstract int getNumberOfQueuedBuffers(int targetSubpartition);

    public void setMaxOverdraftBuffersPerGate(int maxOverdraftBuffersPerGate) {
        this.bufferPool.setMaxOverdraftBuffersPerGate(maxOverdraftBuffersPerGate);
    }

    /**
     * Returns the type of this result partition.
     *
     * @return result partition type
     */
    //返回此结果分区的类型。
    public ResultPartitionType getPartitionType() {
        return partitionType;
    }

    public ResultPartitionBytesCounter getResultPartitionBytes() {
        return resultPartitionBytes;
    }

    // ------------------------------------------------------------------------

    @Override
    public void notifyEndOfData(StopMode mode) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> getAllDataProcessedFuture() {
        throw new UnsupportedOperationException();
    }

    /**
     * The subpartition notifies that the corresponding downstream task have processed all the user
     * records.
     *
     * @see EndOfData
     * @param subpartition The index of the subpartition sending the notification.
     */
    //子分区通知相应的下游任务已处理完所有用户记录。
    public void onSubpartitionAllDataProcessed(int subpartition) {}

    /**
     * Finishes the result partition.
     *
     * <p>After this operation, it is not possible to add further data to the result partition.
     *
     * <p>For BLOCKING results, this will trigger the deployment of consuming tasks.
     */
    //完成结果分区。
    //执行此操作后，无法向结果分区添加更多数据。
    //对于 BLOCKING 结果，这将触发消耗任务的部署。
    @Override
    public void finish() throws IOException {
        checkInProduceState();

        isFinished = true;
    }

    @Override
    public boolean isFinished() {
        return isFinished;
    }

    public void release() {
        release(null);
    }

    @Override
    public void release(Throwable cause) {
        if (isReleased.compareAndSet(false, true)) {
            LOG.debug("{}: Releasing {}.", owningTaskName, this);

            // Set the error cause
            if (cause != null) {
                this.cause = cause;
            }

            releaseInternal();
        }
    }

    /** Releases all produced data including both those stored in memory and persisted on disk. */
    //释放所有生成的数据，包括存储在内存中和保留在磁盘上的数据。
    protected abstract void releaseInternal();

    private void closeBufferPool() {
        if (bufferPool != null) {
            bufferPool.lazyDestroy();
        }
    }

    @Override
    public void close() {
        closeBufferPool();
    }

    @Override
    public void fail(@Nullable Throwable throwable) {
        // the task canceler thread will call this method to early release the output buffer pool
        //任务取消线程将调用此方法提前释放输出缓冲池
        closeBufferPool();
        partitionManager.releasePartition(partitionId, throwable);
    }

    public Throwable getFailureCause() {
        return cause;
    }

    @Override
    public int getNumTargetKeyGroups() {
        return numTargetKeyGroups;
    }

    @Override
    public void setMetricGroup(TaskIOMetricGroup metrics) {
        numBytesOut = metrics.getNumBytesOutCounter();
        numBuffersOut = metrics.getNumBuffersOutCounter();
        metrics.registerResultPartitionBytesCounter(
                partitionId.getPartitionId(), resultPartitionBytes);
    }

    @Override
    public ResultSubpartitionView createSubpartitionView(
            ResultSubpartitionIndexSet indexSet, BufferAvailabilityListener availabilityListener)
            throws IOException {
        if (indexSet.size() == 1) {
            return createSubpartitionView(
                    indexSet.values().iterator().next(), availabilityListener);
        } else {
            UnionResultSubpartitionView unionView =
                    new UnionResultSubpartitionView(availabilityListener, indexSet.size());
            try {
                for (int i : indexSet.values()) {
                    ResultSubpartitionView view = createSubpartitionView(i, unionView);
                    unionView.notifyViewCreated(i, view);
                }
                return unionView;
            } catch (Exception e) {
                unionView.releaseAllResources();
                throw e;
            }
        }
    }

    /**
     * Returns a reader for the subpartition with the given index.
     *
     * <p>Given that the function to merge outputs from multiple subpartition views is supported
     * uniformly in {@link UnionResultSubpartitionView}, subclasses of {@link ResultPartition} only
     * needs to take care of creating subpartition view for a single subpartition.
     */
    //返回具有给定索引的子分区的读取器。
    //由于UnionResultSubpartitionView统一支持合并多个子分区视图输出的功能，
    //因此ResultPartition的子类只需要负责为单个子分区创建子分区视图。
    protected abstract ResultSubpartitionView createSubpartitionView(
            int index, BufferAvailabilityListener availabilityListener) throws IOException;

    /**
     * Whether this partition is released.
     *
     * <p>A partition is released when each subpartition is either consumed and communication is
     * closed by consumer or failed. A partition is also released if task is cancelled.
     */
    //该分区是否被释放。
    //当每个子分区被消耗并且通信被消费者关闭或失败时，分区被释放。如果任务被取消，分区也会被释放。
    @Override
    public boolean isReleased() {
        return isReleased.get();
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        return bufferPool.getAvailableFuture();
    }

    @Override
    public String toString() {
        return "ResultPartition "
                + partitionId.toString()
                + " ["
                + partitionType
                + ", "
                + numSubpartitions
                + " subpartitions]";
    }

    // ------------------------------------------------------------------------

    /** Notification when a subpartition is released. */
    //释放子分区时的通知。
    void onConsumedSubpartition(int subpartitionIndex) {

        if (isReleased.get()) {
            return;
        }

        LOG.debug(
                "{}: Received release notification for subpartition {}.", this, subpartitionIndex);
    }

    // ------------------------------------------------------------------------

    protected void checkInProduceState() throws IllegalStateException {
        checkState(!isFinished, "Partition already finished.");
    }

    @VisibleForTesting
    public ResultPartitionManager getPartitionManager() {
        return partitionManager;
    }

    /**
     * Whether the buffer can be compressed or not. Note that event is not compressed because it is
     * usually small and the size can become even larger after compression.
     */
    //缓冲区是否可以压缩。请注意，事件不会被压缩，因为它通常很小，并且压缩后大小可能会变得更大。
    protected boolean canBeCompressed(Buffer buffer) {
        return bufferCompressor != null && buffer.isBuffer() && buffer.readableBytes() > 0;
    }
}
