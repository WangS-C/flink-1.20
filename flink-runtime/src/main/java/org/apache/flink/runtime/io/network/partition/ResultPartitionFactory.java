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
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions.CompressionCodec;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.disk.FileChannelManager;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferPoolFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.HsResultPartition;
import org.apache.flink.runtime.io.network.partition.hybrid.HybridShuffleConfiguration;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.shuffle.TieredResultPartitionFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemorySpec;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierFactory;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor;
import org.apache.flink.runtime.shuffle.NettyShuffleUtils;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.ProcessorArchitecture;
import org.apache.flink.util.function.SupplierWithException;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** Factory for {@link ResultPartition} to use in {@link NettyShuffleEnvironment}. */
public class ResultPartitionFactory {

    private static final Logger LOG = LoggerFactory.getLogger(ResultPartitionFactory.class);

    private final ResultPartitionManager partitionManager;

    private final FileChannelManager channelManager;

    private final BufferPoolFactory bufferPoolFactory;

    private final BatchShuffleReadBufferPool batchShuffleReadBufferPool;

    private final ScheduledExecutorService batchShuffleReadIOExecutor;

    private final BoundedBlockingSubpartitionType blockingSubpartitionType;

    private final int configuredNetworkBuffersPerChannel;

    private final int floatingNetworkBuffersPerGate;

    private final int networkBufferSize;

    private final boolean batchShuffleCompressionEnabled;

    private final CompressionCodec compressionCodec;

    private final int maxBuffersPerChannel;

    private final int sortShuffleMinBuffers;

    private final int sortShuffleMinParallelism;

    private final int hybridShuffleSpilledIndexRegionGroupSize;

    private final long hybridShuffleNumRetainedInMemoryRegionsMax;

    private final boolean sslEnabled;

    private final int maxOverdraftBuffersPerGate;

    private final Optional<TieredResultPartitionFactory> tieredStorage;

    public ResultPartitionFactory(
            ResultPartitionManager partitionManager,
            FileChannelManager channelManager,
            BufferPoolFactory bufferPoolFactory,
            BatchShuffleReadBufferPool batchShuffleReadBufferPool,
            ScheduledExecutorService batchShuffleReadIOExecutor,
            BoundedBlockingSubpartitionType blockingSubpartitionType,
            int configuredNetworkBuffersPerChannel,
            int floatingNetworkBuffersPerGate,
            int networkBufferSize,
            boolean batchShuffleCompressionEnabled,
            CompressionCodec compressionCodec,
            int maxBuffersPerChannel,
            int sortShuffleMinBuffers,
            int sortShuffleMinParallelism,
            boolean sslEnabled,
            int maxOverdraftBuffersPerGate,
            int hybridShuffleSpilledIndexRegionGroupSize,
            long hybridShuffleNumRetainedInMemoryRegionsMax,
            Optional<TieredResultPartitionFactory> tieredStorage) {

        this.partitionManager = partitionManager;
        this.channelManager = channelManager;
        this.configuredNetworkBuffersPerChannel = configuredNetworkBuffersPerChannel;
        this.floatingNetworkBuffersPerGate = floatingNetworkBuffersPerGate;
        this.bufferPoolFactory = bufferPoolFactory;
        this.batchShuffleReadBufferPool = batchShuffleReadBufferPool;
        this.batchShuffleReadIOExecutor = batchShuffleReadIOExecutor;
        this.blockingSubpartitionType = blockingSubpartitionType;
        this.networkBufferSize = networkBufferSize;
        this.batchShuffleCompressionEnabled = batchShuffleCompressionEnabled;
        this.compressionCodec = compressionCodec;
        this.maxBuffersPerChannel = maxBuffersPerChannel;
        this.sortShuffleMinBuffers = sortShuffleMinBuffers;
        this.sortShuffleMinParallelism = sortShuffleMinParallelism;
        this.sslEnabled = sslEnabled;
        this.maxOverdraftBuffersPerGate = maxOverdraftBuffersPerGate;
        this.hybridShuffleSpilledIndexRegionGroupSize = hybridShuffleSpilledIndexRegionGroupSize;
        this.hybridShuffleNumRetainedInMemoryRegionsMax =
                hybridShuffleNumRetainedInMemoryRegionsMax;
        this.tieredStorage = tieredStorage;
    }

    public ResultPartition create(
            String taskNameWithSubtaskAndId,
            int partitionIndex,
            ResultPartitionDeploymentDescriptor desc) {
        return create(
                taskNameWithSubtaskAndId,
                partitionIndex,
                desc.getShuffleDescriptor().getResultPartitionID(),
                desc.getPartitionType(),
                desc.getTotalNumberOfPartitions(),
                //返回下游一个ExecutionJobVertex中有多少个ExecutionVertex实例消费该Task实例的数据
                desc.getNumberOfSubpartitions(),
                desc.getMaxParallelism(),
                //中间结果是否为广播结果。
                desc.isBroadcast(),
                desc.getShuffleDescriptor(),

                //创建BufferPool工厂
                createBufferPoolFactory(
                        //代表下游同一个算子有多少个算子实例消费该Task数据。
                        desc.getNumberOfSubpartitions(), desc.getPartitionType()),
                desc.isNumberOfPartitionConsumerUndefined());
    }

    @VisibleForTesting
    public ResultPartition create(
            String taskNameWithSubtaskAndId,
            int partitionIndex,
            ResultPartitionID id,
            ResultPartitionType type,
            int numberOfPartitions,
            int numberOfSubpartitions,
            int maxParallelism,
            boolean isBroadcast,
            ShuffleDescriptor shuffleDescriptor,
            SupplierWithException<BufferPool, IOException> bufferPoolFactory,
            boolean isNumberOfPartitionConsumerUndefined) {
        BufferCompressor bufferCompressor = null;
        if (type.supportCompression() && batchShuffleCompressionEnabled) {
            //创建Buffer的压缩器。
            bufferCompressor = new BufferCompressor(networkBufferSize, compressionCodec);
        }
        if (tieredStorage.isPresent() && type == ResultPartitionType.BLOCKING) {
            LOG.warn(
                    "When enabling tiered storage, the BLOCKING result partition will be replaced as HYBRID_FULL.");
            type = ResultPartitionType.HYBRID_FULL;
        }

        ResultSubpartition[] subpartitions = new ResultSubpartition[numberOfSubpartitions];

        final ResultPartition partition;
        if (type == ResultPartitionType.PIPELINED
                || type == ResultPartitionType.PIPELINED_BOUNDED
                || type == ResultPartitionType.PIPELINED_APPROXIMATE) {
            //首先创建隶属于该Task的ResultPartition实例，
            final PipelinedResultPartition pipelinedPartition =
                    new PipelinedResultPartition(
                            taskNameWithSubtaskAndId,
                            partitionIndex,
                            id,
                            type,
                            subpartitions,
                            maxParallelism,
                            partitionManager,
                            bufferCompressor,
                            bufferPoolFactory);

            for (int i = 0; i < subpartitions.length; i++) {
                if (type == ResultPartitionType.PIPELINED_APPROXIMATE) {
                    subpartitions[i] =
                            new PipelinedApproximateSubpartition(
                                    i, configuredNetworkBuffersPerChannel, pipelinedPartition);
                } else {
                    //根据numberOfSubpartitions参数，遍历生成ResultSubpartitions数组成员。
                    //ResultSubpartitions数组是ResultPartition实例的成员变量。
                    subpartitions[i] =
                            new PipelinedSubpartition(
                                    i, configuredNetworkBuffersPerChannel, pipelinedPartition);
                }
            }

            partition = pipelinedPartition;
        } else if (type == ResultPartitionType.BLOCKING
                || type == ResultPartitionType.BLOCKING_PERSISTENT) {
            if (numberOfSubpartitions >= sortShuffleMinParallelism) {
                partition =
                        new SortMergeResultPartition(
                                taskNameWithSubtaskAndId,
                                partitionIndex,
                                id,
                                type,
                                subpartitions.length,
                                maxParallelism,
                                batchShuffleReadBufferPool,
                                batchShuffleReadIOExecutor,
                                partitionManager,
                                channelManager.createChannel().getPath(),
                                bufferCompressor,
                                bufferPoolFactory);
            } else {
                final BoundedBlockingResultPartition blockingPartition =
                        new BoundedBlockingResultPartition(
                                taskNameWithSubtaskAndId,
                                partitionIndex,
                                id,
                                type,
                                subpartitions,
                                maxParallelism,
                                partitionManager,
                                bufferCompressor,
                                bufferPoolFactory);

                initializeBoundedBlockingPartitions(
                        subpartitions,
                        blockingPartition,
                        blockingSubpartitionType,
                        networkBufferSize,
                        channelManager,
                        sslEnabled);

                partition = blockingPartition;
            }
        } else if (type == ResultPartitionType.HYBRID_FULL
                || type == ResultPartitionType.HYBRID_SELECTIVE) {
            checkState(shuffleDescriptor instanceof NettyShuffleDescriptor);
            if (tieredStorage.isPresent()) {
                partition =
                        tieredStorage
                                .get()
                                .createTieredResultPartition(
                                        taskNameWithSubtaskAndId,
                                        partitionIndex,
                                        id,
                                        type,
                                        numberOfPartitions,
                                        subpartitions.length,
                                        maxParallelism,
                                        networkBufferSize,
                                        isBroadcast,
                                        partitionManager,
                                        bufferCompressor,
                                        checkNotNull(
                                                ((NettyShuffleDescriptor) shuffleDescriptor)
                                                        .getTierShuffleDescriptors()),
                                        bufferPoolFactory,
                                        channelManager,
                                        batchShuffleReadBufferPool,
                                        batchShuffleReadIOExecutor,
                                        isNumberOfPartitionConsumerUndefined);
            } else {
                partition =
                        new HsResultPartition(
                                taskNameWithSubtaskAndId,
                                partitionIndex,
                                id,
                                type,
                                subpartitions.length,
                                maxParallelism,
                                batchShuffleReadBufferPool,
                                batchShuffleReadIOExecutor,
                                partitionManager,
                                channelManager.createChannel().getPath(),
                                networkBufferSize,
                                getHybridShuffleConfiguration(numberOfSubpartitions, type),
                                bufferCompressor,
                                isBroadcast,
                                bufferPoolFactory);
            }
        } else {
            throw new IllegalArgumentException("Unrecognized ResultPartitionType: " + type);
        }

        partition.isNumberOfPartitionConsumerUndefined(isNumberOfPartitionConsumerUndefined);

        LOG.debug("{}: Initialized {}", taskNameWithSubtaskAndId, this);

        return partition;
    }

    private HybridShuffleConfiguration getHybridShuffleConfiguration(
            int numberOfSubpartitions, ResultPartitionType resultPartitionType) {
        return HybridShuffleConfiguration.builder(
                        numberOfSubpartitions, batchShuffleReadBufferPool.getNumBuffersPerRequest())
                .setSpillingStrategyType(
                        resultPartitionType == ResultPartitionType.HYBRID_FULL
                                ? HybridShuffleConfiguration.SpillingStrategyType.FULL
                                : HybridShuffleConfiguration.SpillingStrategyType.SELECTIVE)
                .setRegionGroupSizeInBytes(hybridShuffleSpilledIndexRegionGroupSize)
                .setNumRetainedInMemoryRegionsMax(hybridShuffleNumRetainedInMemoryRegionsMax)
                .build();
    }

    private static void initializeBoundedBlockingPartitions(
            ResultSubpartition[] subpartitions,
            BoundedBlockingResultPartition parent,
            BoundedBlockingSubpartitionType blockingSubpartitionType,
            int networkBufferSize,
            FileChannelManager channelManager,
            boolean sslEnabled) {
        int i = 0;
        try {
            for (i = 0; i < subpartitions.length; i++) {
                final File spillFile = channelManager.createChannel().getPathFile();
                subpartitions[i] =
                        blockingSubpartitionType.create(
                                i, parent, spillFile, networkBufferSize, sslEnabled);
            }
        } catch (IOException e) {
            // undo all the work so that a failed constructor does not leave any resources
            // in need of disposal
            releasePartitionsQuietly(subpartitions, i);

            // this is not good, we should not be forced to wrap this in a runtime exception.
            // the fact that the ResultPartition and Task constructor (which calls this) do not
            // tolerate any exceptions
            // is incompatible with eager initialization of resources (RAII).
            throw new FlinkRuntimeException(e);
        }
    }

    private static void releasePartitionsQuietly(ResultSubpartition[] partitions, int until) {
        for (int i = 0; i < until; i++) {
            final ResultSubpartition subpartition = partitions[i];
            ExceptionUtils.suppressExceptions(subpartition::release);
        }
    }

    /** Return whether this result partition need overdraft buffer. */
    //返回该结果分区是否需要透支缓冲区。
    private static boolean isOverdraftBufferNeeded(ResultPartitionType resultPartitionType) {
        // Only pipelined / pipelined-bounded partition needs overdraft buffer. More
        // specifically, there is no reason to request more buffers for non-pipelined (i.e.
        // batch) shuffle. The reasons are as follows:
        // 1. For BoundedBlockingShuffle, each full buffer will be directly released.
        // 2. For SortMergeShuffle, the maximum capacity of buffer pool is 4 * numSubpartitions. It
        // is efficient enough to spill this part of memory to disk.
        // 3. For Hybrid Shuffle, the buffer pool is unbounded. If it can't get a normal buffer, it
        // also can't get an overdraft buffer.
        //只有流水线流水线分区才需要透支缓冲区。更具体地说，没有理由为非流水线（即批量）洗牌请求更多缓冲区。
        // 原因如下：
        // 1、对于BoundedBlockingShuffle来说，每个满的buffer都会被直接释放。
        // 2. 对于SortMergeShuffle，缓冲池的最大容量为4 numSubpartitions。将这部分内存溢出到磁盘是足够高效的。
        // 3. 对于Hybrid Shuffle，缓冲池是无界的。如果它无法获得正常缓冲区，它也无法获得透支缓冲区。
        return resultPartitionType.isPipelinedOrPipelinedBoundedResultPartition();
    }

    /**
     * The minimum pool size should be <code>numberOfSubpartitions + 1</code> for two
     * considerations:
     *
     * <p>1. StreamTask can only process input if there is at-least one available buffer on output
     * side, so it might cause stuck problem if the minimum pool size is exactly equal to the number
     * of subpartitions, because every subpartition might maintain a partial unfilled buffer.
     *
     * <p>2. Increases one more buffer for every output LocalBufferPool to avoid performance
     * regression if processing input is based on at-least one buffer available on output side.
     */
   //出于两个考虑，最小池大小应为numberOfSubpartitions 1:
    //1. StreamTask只能在输出侧至少有一个可用缓冲区的情况下处理输入，
    // 因此如果最小池大小恰好等于子分区的数量，则可能会导致卡住问题，因为每个子分区都可能维护部分未填充的缓冲区。
    //2.为每个输出LocalBufferPool增加一个缓冲区，以避免性能回归，如果处理输入是基于至少一个缓冲区可用在输出侧
    @VisibleForTesting
    SupplierWithException<BufferPool, IOException> createBufferPoolFactory(
            int numberOfSubpartitions, ResultPartitionType type) {
        return () -> {
            Pair<Integer, Integer> pair =
                    //计算并返回结果分区使用的本地网络缓冲池大小
                    NettyShuffleUtils.getMinMaxNetworkBuffersPerResultPartition(
                            configuredNetworkBuffersPerChannel,
                            floatingNetworkBuffersPerGate,
                            sortShuffleMinParallelism,
                            sortShuffleMinBuffers,
                            numberOfSubpartitions,
                            tieredStorage.isPresent(),
                            tieredStorage
                                    .map(ResultPartitionFactory::getNumTotalGuaranteedBuffers)
                                    .orElse(0),
                            type);

            return bufferPoolFactory.createBufferPool(
                    pair.getLeft(),
                    pair.getRight(),
                    numberOfSubpartitions,
                    maxBuffersPerChannel,
                    //返回该结果分区是否需要透支缓冲区。
                    isOverdraftBufferNeeded(type) ? maxOverdraftBuffersPerGate : 0);
        };
    }

    static BoundedBlockingSubpartitionType getBoundedBlockingType() {
        switch (ProcessorArchitecture.getMemoryAddressSize()) {
            case _64_BIT:
                return BoundedBlockingSubpartitionType.FILE_MMAP;
            case _32_BIT:
                return BoundedBlockingSubpartitionType.FILE;
            default:
                LOG.warn("Cannot determine memory architecture. Using pure file-based shuffle.");
                return BoundedBlockingSubpartitionType.FILE;
        }
    }

    private static int getNumTotalGuaranteedBuffers(
            TieredResultPartitionFactory resultPartitionFactory) {
        return resultPartitionFactory.getTieredStorageConfiguration().getTierFactories().stream()
                .map(TierFactory::getProducerAgentMemorySpec)
                .map(TieredStorageMemorySpec::getNumGuaranteedBuffers)
                .mapToInt(Integer::intValue)
                .sum();
    }
}
