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

package org.apache.flink.runtime.shuffle;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Intermediate result partition registry to use in {@link
 * org.apache.flink.runtime.jobmaster.JobMaster}.
 *
 * @param <T> partition shuffle descriptor used for producer/consumer deployment and their data
 *     exchange.
 */
public interface ShuffleMaster<T extends ShuffleDescriptor> extends AutoCloseable {

    /**
     * Starts this shuffle master as a service. One can do some initialization here, for example
     * getting access and connecting to the external system.
     */
    default void start() throws Exception {}

    /**
     * Closes this shuffle master service which should release all resources. A shuffle master will
     * only be closed when the cluster is shut down.
     */
    @Override
    default void close() throws Exception {}

    /**
     * Registers the target job together with the corresponding {@link JobShuffleContext} to this
     * shuffle master. Through the shuffle context, one can obtain some basic information like job
     * ID, job configuration. It enables ShuffleMaster to notify JobMaster about lost result
     * partitions, so that JobMaster can identify and reproduce unavailable partitions earlier.
     *
     * @param context the corresponding shuffle context of the target job.
     */
    //将目标作业与相应的JobShuffleContext一起注册到此 shuffle master。
    //通过shuffle context，我们可以获取一些基本信息，比如作业ID、作业配置等。
    //它使ShuffleMaster能够向JobMaster通知丢失的结果分区，以便JobMaster能够更早地识别和重现不可用的分区。
    //参数：
    //context – 目标作业对应的 shuffle 上下文。
    default void registerJob(JobShuffleContext context) {}

    /**
     * Unregisters the target job from this shuffle master, which means the corresponding job has
     * reached a global termination state and all the allocated resources except for the cluster
     * partitions can be cleared.
     *
     * @param jobID ID of the target job to be unregistered.
     */
    default void unregisterJob(JobID jobID) {}

    /**
     * Asynchronously register a partition and its producer with the shuffle service.
     *
     * <p>The returned shuffle descriptor is an internal handle which identifies the partition
     * internally within the shuffle service. The descriptor should provide enough information to
     * read from or write data to the partition.
     *
     * @param jobID job ID of the corresponding job which registered the partition
     * @param partitionDescriptor general job graph information about the partition
     * @param producerDescriptor general producer information (location, execution id, connection
     *     info)
     * @return future with the partition shuffle descriptor used for producer/consumer deployment
     *     and their data exchange.
     */
    //使用 shuffle 服务异步注册分区及其生产者。
    //返回的 shuffle 描述符是一个内部句柄，用于标识 shuffle 服务内部的分区。描述符应提供足够的信息来从分区读取数据或向分区写入数据。
    //参数：
    //jobID – 注册分区的相应作业的作业 ID
    //partitionDescriptor – 有关分区的一般作业图信息
    //producerDescriptor – 一般生产者信息（位置、执行 ID、连接信息）
    CompletableFuture<T> registerPartitionWithProducer(
            JobID jobID,
            PartitionDescriptor partitionDescriptor,
            ProducerDescriptor producerDescriptor);

    /**
     * Release any external resources occupied by the given partition.
     *
     * <p>This call triggers release of any resources which are occupied by the given partition in
     * the external systems outside of the producer executor. This is mostly relevant for the batch
     * jobs and blocking result partitions. The producer local resources are managed by {@link
     * ShuffleDescriptor#storesLocalResourcesOn()} and {@link
     * ShuffleEnvironment#releasePartitionsLocally(Collection)}.
     *
     * @param shuffleDescriptor shuffle descriptor of the result partition to release externally.
     */
    void releasePartitionExternally(ShuffleDescriptor shuffleDescriptor);

    /**
     * Compute shuffle memory size for a task with the given {@link TaskInputsOutputsDescriptor}.
     *
     * @param taskInputsOutputsDescriptor describes task inputs and outputs information for shuffle
     *     memory calculation.
     * @return shuffle memory size for a task with the given {@link TaskInputsOutputsDescriptor}.
     */
    default MemorySize computeShuffleMemorySizeForTask(
            TaskInputsOutputsDescriptor taskInputsOutputsDescriptor) {
        return MemorySize.ZERO;
    }

    /**
     * Retrieves specified partitions and their metrics (identified by {@code expectedPartitions}),
     * the metrics include sizes of sub-partitions in a result partition.
     *
     * @param jobId ID of the target job
     * @param timeout The timeout used for retrieve the specified partitions.
     * @param expectedPartitions The set of identifiers for the result partitions whose metrics are
     *     to be fetched.
     * @return A future will contain a collection of the partitions with their metrics that could be
     *     retrieved from the expected partitions within the specified timeout period.
     */
    default CompletableFuture<Collection<PartitionWithMetrics>> getPartitionWithMetrics(
            JobID jobId, Duration timeout, Set<ResultPartitionID> expectedPartitions) {
        return CompletableFuture.completedFuture(Collections.emptyList());
    }

    /**
     * Whether the shuffle master supports taking snapshot in batch scenarios if {@link
     * org.apache.flink.configuration.BatchExecutionOptions#JOB_RECOVERY_ENABLED} is true. If it
     * returns true, Flink will call {@link #snapshotState} to take snapshot, and call {@link
     * #restoreState} to restore the state of shuffle master.
     */
    default boolean supportsBatchSnapshot() {
        return false;
    }

    /** Triggers a snapshot of the shuffle master's state. */
    default void snapshotState(
            CompletableFuture<ShuffleMasterSnapshot> snapshotFuture,
            ShuffleMasterSnapshotContext context) {}

    /** Restores the state of the shuffle master from the provided snapshots. */
    default void restoreState(List<ShuffleMasterSnapshot> snapshots) {}

    /**
     * Notifies that the recovery process of result partitions has started.
     *
     * @param jobId ID of the target job
     */
    default void notifyPartitionRecoveryStarted(JobID jobId) {}
}
