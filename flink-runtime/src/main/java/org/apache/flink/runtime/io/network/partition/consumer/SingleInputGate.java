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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.core.memory.MemorySegmentProvider;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.RecoveryMetadata;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferDecompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.partition.PartitionProducerStateProvider;
import org.apache.flink.runtime.io.network.partition.PrioritizedDeque;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageIdMappingUtils;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageInputChannelId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyServiceImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.AvailabilityNotifier;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageConsumerClient;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageConsumerSpec;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor;
import org.apache.flink.runtime.throughput.BufferDebloater;
import org.apache.flink.runtime.throughput.ThroughputCalculator;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.SupplierWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An input gate consumes one or more partitions of a single produced intermediate result.
 *
 * <p>Each intermediate result is partitioned over its producing parallel subtasks; each of these
 * partitions is furthermore partitioned into one or more subpartitions.
 *
 * <p>As an example, consider a map-reduce program, where the map operator produces data and the
 * reduce operator consumes the produced data.
 *
 * <pre>{@code
 * +-----+              +---------------------+              +--------+
 * | Map | = produce => | Intermediate Result | <= consume = | Reduce |
 * +-----+              +---------------------+              +--------+
 * }</pre>
 *
 * <p>When deploying such a program in parallel, the intermediate result will be partitioned over
 * its producing parallel subtasks; each of these partitions is furthermore partitioned into one or
 * more subpartitions.
 *
 * <pre>{@code
 *                            Intermediate result
 *               +-----------------------------------------+
 *               |                      +----------------+ |              +-----------------------+
 * +-------+     | +-------------+  +=> | Subpartition 1 | | <=======+=== | Input Gate | Reduce 1 |
 * | Map 1 | ==> | | Partition 1 | =|   +----------------+ |         |    +-----------------------+
 * +-------+     | +-------------+  +=> | Subpartition 2 | | <==+    |
 *               |                      +----------------+ |    |    | Subpartition request
 *               |                                         |    |    |
 *               |                      +----------------+ |    |    |
 * +-------+     | +-------------+  +=> | Subpartition 1 | | <==+====+
 * | Map 2 | ==> | | Partition 2 | =|   +----------------+ |    |         +-----------------------+
 * +-------+     | +-------------+  +=> | Subpartition 2 | | <==+======== | Input Gate | Reduce 2 |
 *               |                      +----------------+ |              +-----------------------+
 *               +-----------------------------------------+
 * }</pre>
 *
 * <p>In the above example, two map subtasks produce the intermediate result in parallel, resulting
 * in two partitions (Partition 1 and 2). Each of these partitions is further partitioned into two
 * subpartitions -- one for each parallel reduce subtask.
 */
//输入门消耗单个产生的中间结果的一个或多个分区。
//每个中间结果在其产生的并行子任务上被分区; 这些分区中的每一个进一步被分区成一个或多个子分区。
//作为示例，考虑map-reduce程序，其中map运算符产生数据并且reduce运算符消耗所产生的数据。
//+-----+              +---------------------+              +--------+ | Map | = produce => | Intermediate Result | <= consume = | Reduce | +-----+              +---------------------+              +--------+
//当并行部署这样的程序时，中间结果将在其产生的并行子任务上被分区; 这些分区中的每一个进一步被分区成一个或多个子分区。
//                           Intermediate result               +-----------------------------------------+               |                      +----------------+ |              +-----------------------+ +-------+     | +-------------+  +=> | Subpartition 1 | | <=======+=== | Input Gate | Reduce 1 | | Map 1 | ==> | | Partition 1 | =|   +----------------+ |         |    +-----------------------+ +-------+     | +-------------+  +=> | Subpartition 2 | | <==+    |               |                      +----------------+ |    |    | Subpartition request               |                                         |    |    |               |                      +----------------+ |    |    | +-------+     | +-------------+  +=> | Subpartition 1 | | <==+====+ | Map 2 | ==> | | Partition 2 | =|   +----------------+ |    |         +-----------------------+ +-------+     | +-------------+  +=> | Subpartition 2 | | <==+======== | Input Gate | Reduce 2 |               |                      +----------------+ |              +-----------------------+               +-----------------------------------------+
//在上面的示例中，两个映射子任务并行地产生中间结果，从而产生两个分区 (分区1和分区2)。这些分区中的每一个都进一步划分为两个子分区-每个并行reduce子任务一个
public class SingleInputGate extends IndexedInputGate {

    private static final Logger LOG = LoggerFactory.getLogger(SingleInputGate.class);

    /** Lock object to guard partition requests and runtime channel updates. */
    //锁定对象以保护分区请求和运行时通道更新。
    private final Object requestLock = new Object();

    /** The name of the owning task, for logging purposes. */
    //所属任务的名称，用于记录目的
    private final String owningTaskName;

    //代表当前Task消费的上游Task的下标，大部分情况下一个算子只有一个上游输入，如果有多个上游输入，gateIndex变量标识哪个上游输入。
    private final int gateIndex;

    /**
     * The ID of the consumed intermediate result. Each input gate consumes partitions of the
     * intermediate result specified by this ID. This ID also identifies the input gate at the
     * consuming task.
     */
    //代表当前Task消费的上游算子的中间结果集。
    private final IntermediateDataSetID consumedResultId;

    /** The type of the partition the input gate is consuming. */
    //代表当前InputGate的消费分区类型，Flink流式应用一般都是PIPELINED_BOUNDED模式，采用有限个buffer缓存来支持上下游同时生产和消费数据。
    private final ResultPartitionType consumedPartitionType;

    /** The number of input channels (equivalent to the number of consumed partitions). */
    //代表有多少个上游结果子分区输入。
    private final int numberOfInputChannels;

    /** Input channels. We store this in a map for runtime updates of single channels. */
    //代表上游结果子分区输入明细。
    private final Map<IntermediateResultPartitionID, Map<InputChannelInfo, InputChannel>>
            inputChannels;

    //代表上游结果子分区输入明细。
    @GuardedBy("requestLock")
    private final InputChannel[] channels;

    /** Channels, which notified this input gate about available data. */
    //当前可读取数据的InputChannel。
    private final PrioritizedDeque<InputChannel> inputChannelsWithData = new PrioritizedDeque<>();

    /**
     * Field guaranteeing uniqueness for inputChannelsWithData queue. Both of those fields should be
     * unified onto one.
     */
    //保证 inputChannelsWithData 队列唯一性的字段。这两个领域应该统一为一个。
    @GuardedBy("inputChannelsWithData")
    private final BitSet enqueuedInputChannelsWithData;

    @GuardedBy("inputChannelsWithData")
    private final BitSet channelsWithEndOfPartitionEvents;

    @GuardedBy("inputChannelsWithData")
    private final BitSet channelsWithEndOfUserRecords;

    @GuardedBy("inputChannelsWithData")
    private int[] lastPrioritySequenceNumber;

    /** The partition producer state listener. */
    //分区生产者状态监听器
    private final PartitionProducerStateProvider partitionProducerStateProvider;

    /**
     * Buffer pool for incoming buffers. Incoming data from remote channels is copied to buffers
     * from this pool.
     */
    //代表SingleInputGate的本地buffer池，用来缓存读取到的数据。
    private BufferPool bufferPool;

    private boolean hasReceivedAllEndOfPartitionEvents;

    private boolean hasReceivedEndOfData;

    /** Flag indicating whether partitions have been requested. */
    //指示是否已请求分区的标志。
    private boolean requestedPartitionsFlag;

    private final List<TaskEvent> pendingEvents = new ArrayList<>();

    private int numberOfUninitializedChannels;

    /** A timer to retrigger local partition requests. Only initialized if actually needed. */
    //用于重新触发本地分区请求的计时器。仅在实际需要时才初始化。
    private Timer retriggerLocalRequestTimer;

    private final SupplierWithException<BufferPool, IOException> bufferPoolFactory;

    private final CompletableFuture<Void> closeFuture;

    @Nullable private final BufferDecompressor bufferDecompressor;

    private final MemorySegmentProvider memorySegmentProvider;

    /**
     * The segment to read data from file region of bounded blocking partition by local input
     * channel.
     */
    //通过本地输入通道从有界阻塞分区的文件区域读取数据的段。
    private final MemorySegment unpooledSegment;

    private final ThroughputCalculator throughputCalculator;
    private final BufferDebloater bufferDebloater;
    private boolean shouldDrainOnEndOfData = true;

    // The consumer client will be null if the tiered storage is not enabled.
    //如果不启用分层存储，消费者客户端将为空。
    @Nullable private TieredStorageConsumerClient tieredStorageConsumerClient;

    // The consumer specs in tiered storage will be null if the tiered storage is not enabled.
    //如果未启用分层存储，则分层存储中的消费者规格将为空。
    @Nullable private List<TieredStorageConsumerSpec> tieredStorageConsumerSpecs;

    // The availability notifier will be null if the tiered storage is not enabled.
    //如果未启用分层存储，则可用性通知程序将为空。
    @Nullable private AvailabilityNotifier availabilityNotifier;

    /**
     * A map containing the status of the last consumed buffer in each input channel. The status
     * contains the following information: 1) whether the buffer contains partial record, and 2) the
     * index of the subpartition where the buffer comes from.
     */
    //包含每个输入通道中最后消耗的缓冲区状态的映射。状态包含以下信息：
    // 1）缓冲区是否包含部分记录，
    // 2）缓冲区来自的子分区的索引。
    private final Map<Integer, Tuple2<Boolean, Integer>> lastBufferStatusMapInTieredStore =
            new HashMap<>();

    /** A map of counters for the number of {@link EndOfData}s received from each input channel. */
    //从每个输入通道接收到的EndOfData数量的计数器映射。
    private final int[] endOfDatas;

    /**
     * A map of counters for the number of {@link EndOfPartitionEvent}s received from each input
     * channel.
     */
    //从每个输入通道接收到的EndOfPartitionEvent数量的计数器映射。
    private final int[] endOfPartitions;

    public SingleInputGate(
            String owningTaskName,
            int gateIndex,
            IntermediateDataSetID consumedResultId,
            final ResultPartitionType consumedPartitionType,
            int numberOfInputChannels,
            PartitionProducerStateProvider partitionProducerStateProvider,
            SupplierWithException<BufferPool, IOException> bufferPoolFactory,
            @Nullable BufferDecompressor bufferDecompressor,
            MemorySegmentProvider memorySegmentProvider,
            int segmentSize,
            ThroughputCalculator throughputCalculator,
            @Nullable BufferDebloater bufferDebloater) {

        this.owningTaskName = checkNotNull(owningTaskName);
        Preconditions.checkArgument(0 <= gateIndex, "The gate index must be positive.");
        this.gateIndex = gateIndex;

        this.consumedResultId = checkNotNull(consumedResultId);
        this.consumedPartitionType = checkNotNull(consumedPartitionType);
        this.bufferPoolFactory = checkNotNull(bufferPoolFactory);

        checkArgument(numberOfInputChannels > 0);
        this.numberOfInputChannels = numberOfInputChannels;

        this.inputChannels = CollectionUtil.newHashMapWithExpectedSize(numberOfInputChannels);
        this.channels = new InputChannel[numberOfInputChannels];
        this.channelsWithEndOfPartitionEvents = new BitSet(numberOfInputChannels);
        this.channelsWithEndOfUserRecords = new BitSet(numberOfInputChannels);
        this.enqueuedInputChannelsWithData = new BitSet(numberOfInputChannels);
        this.lastPrioritySequenceNumber = new int[numberOfInputChannels];
        Arrays.fill(lastPrioritySequenceNumber, Integer.MIN_VALUE);

        this.partitionProducerStateProvider = checkNotNull(partitionProducerStateProvider);

        this.bufferDecompressor = bufferDecompressor;
        this.memorySegmentProvider = checkNotNull(memorySegmentProvider);

        this.closeFuture = new CompletableFuture<>();

        this.unpooledSegment = MemorySegmentFactory.allocateUnpooledSegment(segmentSize);
        this.bufferDebloater = bufferDebloater;
        this.throughputCalculator = checkNotNull(throughputCalculator);

        this.tieredStorageConsumerClient = null;
        this.tieredStorageConsumerSpecs = null;
        this.availabilityNotifier = null;

        this.endOfDatas = new int[numberOfInputChannels];
        Arrays.fill(endOfDatas, 0);
        this.endOfPartitions = new int[numberOfInputChannels];
        Arrays.fill(endOfPartitions, 0);
    }

    protected PrioritizedDeque<InputChannel> getInputChannelsWithData() {
        return inputChannelsWithData;
    }

    @Override
    public void setup() throws IOException {
        checkState(
                this.bufferPool == null,
                "Bug in input gate setup logic: Already registered buffer pool.");

        //调用org.apache.flink.runtime.io.network.buffer.NetworkBufferPool.createBufferPool(int, int)
        BufferPool bufferPool = bufferPoolFactory.get();
        setBufferPool(bufferPool);
        if (tieredStorageConsumerClient != null) {
            tieredStorageConsumerClient.setup(bufferPool);
        }

        //将专用缓冲区直接分配给所有远程输入通道，以实现基于信用的模式。
        setupChannels();
    }

    @Override
    public CompletableFuture<Void> getStateConsumedFuture() {
        synchronized (requestLock) {
            List<CompletableFuture<?>> futures = new ArrayList<>(numberOfInputChannels);
            for (InputChannel inputChannel : inputChannels()) {
                if (inputChannel instanceof RecoveredInputChannel) {
                    futures.add(((RecoveredInputChannel) inputChannel).getStateConsumedFuture());
                }
            }
            return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
        }
    }

    @Override
    public void requestPartitions() {
        synchronized (requestLock) {
            if (!requestedPartitionsFlag) {
                if (closeFuture.isDone()) {
                    throw new IllegalStateException("Already released.");
                }

                // Sanity checks
                // 健全性检查
                long numInputChannels =
                        inputChannels.values().stream().mapToLong(x -> x.values().size()).sum();
                if (numberOfInputChannels != numInputChannels) {
                    throw new IllegalStateException(
                            String.format(
                                    "Bug in input gate setup logic: mismatch between "
                                            + "number of total input channels [%s] and the currently set number of input "
                                            + "channels [%s].",
                                    numInputChannels, numberOfInputChannels));
                }

                convertRecoveredInputChannels();
                //调用internalRequestPartitions
                internalRequestPartitions();
            }

            requestedPartitionsFlag = true;
            // Start the reader only when all InputChannels have been converted to either
            // LocalInputChannel or RemoteInputChannel, as this will prevent RecoveredInputChannels
            // from being queued again.
            // 仅当所有 InputChannel 都已转换为 LocalInputChannel 或 RemoteInputChannel 时才启动读取器，
            // 因为这将阻止 RecoveredInputChannel 再次排队。
            if (enabledTieredStorage()) {
                tieredStorageConsumerClient.start();
            }
        }
    }

    @VisibleForTesting
    public void convertRecoveredInputChannels() {
        LOG.debug("Converting recovered input channels ({} channels)", getNumberOfInputChannels());
        for (Map<InputChannelInfo, InputChannel> inputChannelsForCurrentPartition :
                inputChannels.values()) {
            Set<InputChannelInfo> oldInputChannelInfos =
                    new HashSet<>(inputChannelsForCurrentPartition.keySet());
            for (InputChannelInfo inputChannelInfo : oldInputChannelInfos) {
                InputChannel inputChannel = inputChannelsForCurrentPartition.get(inputChannelInfo);
                if (inputChannel instanceof RecoveredInputChannel) {
                    try {
                        InputChannel realInputChannel =
                                ((RecoveredInputChannel) inputChannel).toInputChannel();
                        inputChannel.releaseAllResources();
                        inputChannelsForCurrentPartition.remove(inputChannelInfo);
                        inputChannelsForCurrentPartition.put(
                                realInputChannel.getChannelInfo(), realInputChannel);
                        channels[inputChannel.getChannelIndex()] = realInputChannel;
                    } catch (Throwable t) {
                        inputChannel.setError(t);
                        return;
                    }
                }
            }
        }
    }

    private void internalRequestPartitions() {
        for (InputChannel inputChannel : inputChannels()) {
            try {
                //请求子分区
                inputChannel.requestSubpartitions();
            } catch (Throwable t) {
                inputChannel.setError(t);
                return;
            }
        }
    }

    @Override
    public void finishReadRecoveredState() throws IOException {
        for (final InputChannel channel : channels) {
            if (channel instanceof RecoveredInputChannel) {
                ((RecoveredInputChannel) channel).finishReadRecoveredState();
            }
        }
    }

    // ------------------------------------------------------------------------
    // Properties
    // ------------------------------------------------------------------------

    @Override
    public int getNumberOfInputChannels() {
        return numberOfInputChannels;
    }

    @Override
    public int getGateIndex() {
        return gateIndex;
    }

    @Override
    public List<InputChannelInfo> getUnfinishedChannels() {
        List<InputChannelInfo> unfinishedChannels =
                new ArrayList<>(
                        numberOfInputChannels - channelsWithEndOfPartitionEvents.cardinality());
        synchronized (inputChannelsWithData) {
            for (int i = channelsWithEndOfPartitionEvents.nextClearBit(0);
                    i < numberOfInputChannels;
                    i = channelsWithEndOfPartitionEvents.nextClearBit(i + 1)) {
                unfinishedChannels.add(getChannel(i).getChannelInfo());
            }
        }

        return unfinishedChannels;
    }

    @VisibleForTesting
    int getBuffersInUseCount() {
        int total = 0;
        for (InputChannel channel : channels) {
            total += channel.getBuffersInUseCount();
        }
        return total;
    }

    @VisibleForTesting
    public void announceBufferSize(int newBufferSize) {
        for (InputChannel channel : channels) {
            if (!channel.isReleased()) {
                channel.announceBufferSize(newBufferSize);
            }
        }
    }

    @Override
    public void triggerDebloating() {
        if (isFinished() || closeFuture.isDone()) {
            return;
        }

        checkState(bufferDebloater != null, "Buffer debloater should not be null");
        final long currentThroughput = throughputCalculator.calculateThroughput();
        bufferDebloater
                .recalculateBufferSize(currentThroughput, getBuffersInUseCount())
                .ifPresent(this::announceBufferSize);
    }

    public Duration getLastEstimatedTimeToConsume() {
        return bufferDebloater.getLastEstimatedTimeToConsumeBuffers();
    }

    /**
     * Returns the type of this input channel's consumed result partition.
     *
     * @return consumed result partition type
     */
    //返回此输入通道的消费结果分区的类型。
    public ResultPartitionType getConsumedPartitionType() {
        return consumedPartitionType;
    }

    BufferProvider getBufferProvider() {
        return bufferPool;
    }

    public BufferPool getBufferPool() {
        return bufferPool;
    }

    MemorySegmentProvider getMemorySegmentProvider() {
        return memorySegmentProvider;
    }

    public String getOwningTaskName() {
        return owningTaskName;
    }

    public int getNumberOfQueuedBuffers() {
        // re-try 3 times, if fails, return 0 for "unknown"
        // 重试3次，如果失败，返回0表示“未知”
        for (int retry = 0; retry < 3; retry++) {
            try {
                int totalBuffers = 0;

                for (InputChannel channel : inputChannels()) {
                    totalBuffers += channel.unsynchronizedGetNumberOfQueuedBuffers();
                }

                return totalBuffers;
            } catch (Exception ex) {
                LOG.debug("Fail to get number of queued buffers :", ex);
            }
        }

        return 0;
    }

    public long getSizeOfQueuedBuffers() {
        // re-try 3 times, if fails, return 0 for "unknown"
        for (int retry = 0; retry < 3; retry++) {
            try {
                long totalSize = 0;

                for (InputChannel channel : inputChannels()) {
                    totalSize += channel.unsynchronizedGetSizeOfQueuedBuffers();
                }

                return totalSize;
            } catch (Exception ex) {
                LOG.debug("Fail to get size of queued buffers :", ex);
            }
        }

        return 0;
    }

    public CompletableFuture<Void> getCloseFuture() {
        return closeFuture;
    }

    @Override
    public InputChannel getChannel(int channelIndex) {
        return channels[channelIndex];
    }

    // ------------------------------------------------------------------------
    // Setup/Life-cycle
    // ------------------------------------------------------------------------

    public void setBufferPool(BufferPool bufferPool) {
        checkState(
                this.bufferPool == null,
                "Bug in input gate setup logic: buffer pool has"
                        + "already been set for this input gate.");

        this.bufferPool = checkNotNull(bufferPool);
    }

    /** Assign the exclusive buffers to all remote input channels directly for credit-based mode. */
    //将专用缓冲区直接分配给所有远程输入通道，以实现基于信用的模式。
    @VisibleForTesting
    public void setupChannels() throws IOException {
        // Allocate enough exclusive and floating buffers to guarantee that job can make progress.
        // Note: An exception will be thrown if there is no buffer available in the given timeout.
        //分配足够的独占和浮动缓冲区以保证作业能够取得进展。注意：如果在给定的超时时间内没有可用的缓冲区，则会抛出异常。

        // First allocate a single floating buffer to avoid potential deadlock when the exclusive
        // buffer is 0. See FLINK-24035 for more information.
        //首先分配单个浮动缓冲区，以避免独占缓冲区为 0 时潜在的死锁。有关更多信息，请参阅 FLINK-24035。
        bufferPool.reserveSegments(1);

        // Next allocate the exclusive buffers per channel when the number of exclusive buffer is
        // larger than 0.
        //接下来，当独占缓冲区的数量大于 0 时，为每个通道分配独占缓冲区。
        synchronized (requestLock) {
            for (InputChannel inputChannel : inputChannels()) {
                inputChannel.setup();
            }
        }
    }

    public void setInputChannels(InputChannel... channels) {
        if (channels.length != numberOfInputChannels) {
            throw new IllegalArgumentException(
                    "Expected "
                            + numberOfInputChannels
                            + " channels, "
                            + "but got "
                            + channels.length);
        }
        synchronized (requestLock) {
            System.arraycopy(channels, 0, this.channels, 0, numberOfInputChannels);
            for (InputChannel inputChannel : channels) {
                if (inputChannels
                                        .computeIfAbsent(
                                                inputChannel.getPartitionId().getPartitionId(),
                                                ignored -> new HashMap<>())
                                        .put(inputChannel.getChannelInfo(), inputChannel)
                                == null
                        && inputChannel instanceof UnknownInputChannel) {

                    numberOfUninitializedChannels++;
                }
            }
        }
    }

    public void setTieredStorageService(
            List<TieredStorageConsumerSpec> tieredStorageConsumerSpecs,
            TieredStorageConsumerClient client,
            TieredStorageNettyServiceImpl nettyService) {
        this.tieredStorageConsumerSpecs = tieredStorageConsumerSpecs;
        this.tieredStorageConsumerClient = client;
        if (client != null) {
            this.availabilityNotifier = new AvailabilityNotifierImpl();
            setupTieredStorageNettyService(nettyService, tieredStorageConsumerSpecs);
            client.registerAvailabilityNotifier(availabilityNotifier);
        }
    }

    public void updateInputChannel(
            ResourceID localLocation, NettyShuffleDescriptor shuffleDescriptor)
            throws IOException, InterruptedException {
        synchronized (requestLock) {
            if (closeFuture.isDone()) {
                // There was a race with a task failure/cancel
                return;
            }

            IntermediateResultPartitionID partitionId =
                    shuffleDescriptor.getResultPartitionID().getPartitionId();

            Map<InputChannelInfo, InputChannel> newInputChannels = new HashMap<>();
            for (InputChannel current : inputChannels.get(partitionId).values()) {
                if (current instanceof UnknownInputChannel) {
                    UnknownInputChannel unknownChannel = (UnknownInputChannel) current;
                    boolean isLocal = shuffleDescriptor.isLocalTo(localLocation);
                    InputChannel newChannel;
                    if (isLocal) {
                        newChannel =
                                unknownChannel.toLocalInputChannel(
                                        shuffleDescriptor.getResultPartitionID());
                    } else {
                        RemoteInputChannel remoteInputChannel =
                                unknownChannel.toRemoteInputChannel(
                                        shuffleDescriptor.getConnectionId(),
                                        shuffleDescriptor.getResultPartitionID());
                        remoteInputChannel.setup();
                        newChannel = remoteInputChannel;
                    }
                    LOG.debug(
                            "{}: Updated unknown input channel to {}.", owningTaskName, newChannel);

                    newInputChannels.put(newChannel.getChannelInfo(), newChannel);
                    channels[current.getChannelIndex()] = newChannel;

                    if (requestedPartitionsFlag) {
                        newChannel.requestSubpartitions();
                    }

                    for (TaskEvent event : pendingEvents) {
                        newChannel.sendTaskEvent(event);
                    }

                    if (--numberOfUninitializedChannels == 0) {
                        pendingEvents.clear();
                    }
                    if (enabledTieredStorage()) {
                        TieredStoragePartitionId tieredStoragePartitionId =
                                TieredStorageIdMappingUtils.convertId(
                                        shuffleDescriptor.getResultPartitionID());
                        TieredStorageConsumerSpec spec =
                                checkNotNull(tieredStorageConsumerSpecs)
                                        .get(current.getChannelIndex());
                        for (int subpartitionId : spec.getSubpartitionIds().values()) {
                            tieredStorageConsumerClient.updateTierShuffleDescriptors(
                                    tieredStoragePartitionId,
                                    spec.getInputChannelId(),
                                    new TieredStorageSubpartitionId(subpartitionId),
                                    checkNotNull(shuffleDescriptor.getTierShuffleDescriptors()));
                        }
                        queueChannel(newChannel, null, false);
                    }
                }
            }

            inputChannels.put(partitionId, newInputChannels);
        }
    }

    /** Retriggers a partition request. */
    //重新触发分区请求。
    public void retriggerPartitionRequest(
            IntermediateResultPartitionID partitionId, InputChannelInfo inputChannelInfo)
            throws IOException {
        synchronized (requestLock) {
            if (!closeFuture.isDone()) {
                final InputChannel ch = inputChannels.get(partitionId).get(inputChannelInfo);

                checkNotNull(ch, "Unknown input channel with ID " + partitionId);

                LOG.debug(
                        "{}: Retriggering partition request {}:{}.",
                        owningTaskName,
                        ch.partitionId,
                        ch.getConsumedSubpartitionIndexSet());

                if (ch.getClass() == RemoteInputChannel.class) {
                    final RemoteInputChannel rch = (RemoteInputChannel) ch;
                    rch.retriggerSubpartitionRequest();
                } else if (ch.getClass() == LocalInputChannel.class) {
                    final LocalInputChannel ich = (LocalInputChannel) ch;

                    if (retriggerLocalRequestTimer == null) {
                        retriggerLocalRequestTimer = new Timer(true);
                    }

                    ich.retriggerSubpartitionRequest(retriggerLocalRequestTimer);
                } else {
                    throw new IllegalStateException(
                            "Unexpected type of channel to retrigger partition: " + ch.getClass());
                }
            }
        }
    }

    @VisibleForTesting
    Timer getRetriggerLocalRequestTimer() {
        return retriggerLocalRequestTimer;
    }

    MemorySegment getUnpooledSegment() {
        return unpooledSegment;
    }

    @Override
    public void close() throws IOException {
        boolean released = false;
        synchronized (requestLock) {
            if (!closeFuture.isDone()) {
                try {
                    LOG.debug("{}: Releasing {}.", owningTaskName, this);

                    if (retriggerLocalRequestTimer != null) {
                        retriggerLocalRequestTimer.cancel();
                    }

                    for (InputChannel inputChannel : inputChannels()) {
                        try {
                            inputChannel.releaseAllResources();
                        } catch (IOException e) {
                            LOG.warn(
                                    "{}: Error during release of channel resources: {}.",
                                    owningTaskName,
                                    e.getMessage(),
                                    e);
                        }
                    }

                    // The buffer pool can actually be destroyed immediately after the
                    // reader received all of the data from the input channels.
                    //实际上，在读取器从输入通道接收到所有数据后，缓冲池可以立即被销毁。
                    if (bufferPool != null) {
                        bufferPool.lazyDestroy();
                    }
                } finally {
                    released = true;
                    closeFuture.complete(null);
                }
            }
        }

        if (released) {
            synchronized (inputChannelsWithData) {
                inputChannelsWithData.notifyAll();
            }
            if (enabledTieredStorage()) {
                tieredStorageConsumerClient.close();
            }
        }
    }

    @Override
    public boolean isFinished() {
        return hasReceivedAllEndOfPartitionEvents;
    }

    @Override
    public EndOfDataStatus hasReceivedEndOfData() {
        if (!hasReceivedEndOfData) {
            return EndOfDataStatus.NOT_END_OF_DATA;
        } else if (shouldDrainOnEndOfData) {
            return EndOfDataStatus.DRAINED;
        } else {
            return EndOfDataStatus.STOPPED;
        }
    }

    @Override
    public String toString() {
        return "SingleInputGate{"
                + "owningTaskName='"
                + owningTaskName
                + '\''
                + ", gateIndex="
                + gateIndex
                + '}';
    }

    // ------------------------------------------------------------------------
    // Consume
    // ------------------------------------------------------------------------

    @Override
    public Optional<BufferOrEvent> getNext() throws IOException, InterruptedException {
        return getNextBufferOrEvent(true);
    }

    @Override
    public Optional<BufferOrEvent> pollNext() throws IOException, InterruptedException {
        return getNextBufferOrEvent(false);
    }

    private Optional<BufferOrEvent> getNextBufferOrEvent(boolean blocking)
            throws IOException, InterruptedException {
        if (hasReceivedAllEndOfPartitionEvents) {
            return Optional.empty();
        }

        if (closeFuture.isDone()) {
            throw new CancelTaskException("Input gate is already closed.");
        }
        //获取可用的InputChannel、数据Buffer等信息。
        Optional<InputWithData<InputChannel, Buffer>> next = waitAndGetNextData(blocking);
        if (!next.isPresent()) {
            throughputCalculator.pauseMeasurement();
            return Optional.empty();
        }

        throughputCalculator.resumeMeasurement();

        InputWithData<InputChannel, Buffer> inputWithData = next.get();
        final BufferOrEvent bufferOrEvent =
                //转换为缓冲区或事件 里面会释放缓冲区
                transformToBufferOrEvent(
                        inputWithData.data,
                        inputWithData.moreAvailable,
                        inputWithData.input,
                        inputWithData.morePriorityEvents);
        throughputCalculator.incomingDataSize(bufferOrEvent.getSize());
        return Optional.of(bufferOrEvent);
    }

    private Optional<InputWithData<InputChannel, Buffer>> waitAndGetNextData(boolean blocking)
            throws IOException, InterruptedException {
        while (true) {
            synchronized (inputChannelsWithData) {
                //获取当前可读取数据的InChannel信息
                Optional<InputChannel> inputChannelOpt = getChannel(blocking);
                if (!inputChannelOpt.isPresent()) {
                    return Optional.empty();
                }

                final InputChannel inputChannel = inputChannelOpt.get();
                //读取恢复或正常缓冲区。
                Optional<Buffer> buffer = readRecoveredOrNormalBuffer(inputChannel);
                if (!buffer.isPresent()) {
                    checkUnavailability();
                    continue;
                }

                int numSubpartitions = inputChannel.getConsumedSubpartitionIndexSet().size();
                if (numSubpartitions > 1) {
                    switch (buffer.get().getDataType()) {
                        case END_OF_DATA:
                            endOfDatas[inputChannel.getChannelIndex()]++;
                            if (endOfDatas[inputChannel.getChannelIndex()] < numSubpartitions) {
                                buffer.get().recycleBuffer();
                                continue;
                            }
                            break;
                        case END_OF_PARTITION:
                            endOfPartitions[inputChannel.getChannelIndex()]++;
                            if (endOfPartitions[inputChannel.getChannelIndex()]
                                    < numSubpartitions) {
                                buffer.get().recycleBuffer();
                                continue;
                            }
                            break;
                        default:
                            break;
                    }
                }

                final boolean morePriorityEvents =
                        inputChannelsWithData.getNumPriorityElements() > 0;
                if (buffer.get().getDataType().hasPriority()) {
                    if (!morePriorityEvents) {
                        priorityAvailabilityHelper.resetUnavailable();
                    }
                }
                checkUnavailability();
                return Optional.of(
                        new InputWithData<>(
                                inputChannel,
                                buffer.get(),
                                !inputChannelsWithData.isEmpty(),
                                morePriorityEvents));
            }
        }
    }

    private Optional<Buffer> readRecoveredOrNormalBuffer(InputChannel inputChannel)
            throws IOException, InterruptedException {
        // Firstly, read the buffers from the recovered channel
        // 首先，从恢复的通道读取缓冲区
        if (inputChannel instanceof RecoveredInputChannel && !inputChannel.isReleased()) {
            //从可读取数据InputChannel中开始获取数据。
            Optional<Buffer> buffer = readBufferFromInputChannel(inputChannel);
            if (!((RecoveredInputChannel) inputChannel).getStateConsumedFuture().isDone()) {
                return buffer;
            }
        }

        //  After the recovered buffers are read, read the normal buffers
        // 读取恢复的缓冲区后，读取正常缓冲区
        return enabledTieredStorage()
                ? readBufferFromTieredStore(inputChannel)
                : readBufferFromInputChannel(inputChannel);
    }

    private Optional<Buffer> readBufferFromInputChannel(InputChannel inputChannel)
            throws IOException, InterruptedException {
        Optional<BufferAndAvailability> bufferAndAvailabilityOpt = inputChannel.getNextBuffer();
        if (!bufferAndAvailabilityOpt.isPresent()) {
            return Optional.empty();
        }
        final BufferAndAvailability bufferAndAvailability = bufferAndAvailabilityOpt.get();
        if (bufferAndAvailability.moreAvailable()) {
            // enqueue the inputChannel at the end to avoid starvation
            //将 inputChannel 添加到最后队列以避免饥饿
            queueChannelUnsafe(inputChannel, bufferAndAvailability.morePriorityEvents());
        }
        if (bufferAndAvailability.hasPriority()) {
            lastPrioritySequenceNumber[inputChannel.getChannelIndex()] =
                    bufferAndAvailability.getSequenceNumber();
        }

        Buffer buffer = bufferAndAvailability.buffer();
        if (buffer.getDataType() == Buffer.DataType.RECOVERY_METADATA) {
            RecoveryMetadata recoveryMetadata =
                    (RecoveryMetadata)
                            EventSerializer.fromSerializedEvent(
                                    buffer.getNioBufferReadable(), getClass().getClassLoader());
            lastBufferStatusMapInTieredStore.put(
                    inputChannel.getChannelIndex(),
                    Tuple2.of(
                            buffer.getDataType().isPartialRecord(),
                            recoveryMetadata.getFinalBufferSubpartitionId()));
        }
        return Optional.of(bufferAndAvailability.buffer());
    }

    private Optional<Buffer> readBufferFromTieredStore(InputChannel inputChannel)
            throws IOException {
        TieredStorageConsumerSpec tieredStorageConsumerSpec =
                checkNotNull(tieredStorageConsumerSpecs).get(inputChannel.getChannelIndex());
        Tuple2<Boolean, Integer> lastBufferStatus =
                lastBufferStatusMapInTieredStore.computeIfAbsent(
                        inputChannel.getChannelIndex(), key -> Tuple2.of(false, -1));
        boolean isLastBufferPartialRecord = lastBufferStatus.f0;
        int lastSubpartitionId = lastBufferStatus.f1;

        while (true) {
            int subpartitionId;
            if (isLastBufferPartialRecord) {
                subpartitionId = lastSubpartitionId;
            } else {
                subpartitionId =
                        checkNotNull(tieredStorageConsumerClient)
                                .peekNextBufferSubpartitionId(
                                        tieredStorageConsumerSpec.getPartitionId(),
                                        tieredStorageConsumerSpec.getSubpartitionIds());
            }

            if (subpartitionId < 0) {
                return Optional.empty();
            }

            // If the data is available in the specific partition and subpartition, read buffer
            // through consumer client.
            // 如果特定分区和子分区中有数据，则通过消费者客户端读取缓冲区。
            Optional<Buffer> buffer =
                    checkNotNull(tieredStorageConsumerClient)
                            .getNextBuffer(
                                    tieredStorageConsumerSpec.getPartitionId(),
                                    new TieredStorageSubpartitionId(subpartitionId));

            if (buffer.isPresent()) {
                if (!(inputChannel instanceof RecoveredInputChannel)) {
                    queueChannel(checkNotNull(inputChannel), null, false);
                }
                lastBufferStatusMapInTieredStore.put(
                        inputChannel.getChannelIndex(),
                        Tuple2.of(buffer.get().getDataType().isPartialRecord(), subpartitionId));
            } else {
                if (!isLastBufferPartialRecord
                        && inputChannel.getConsumedSubpartitionIndexSet().size() > 1) {
                    // Continue to check other subpartitions that have been marked as
                    // available.
                    // 继续检查已标记为可用的其他子分区。
                    continue;
                }
            }

            return buffer;
        }
    }

    private boolean enabledTieredStorage() {
        return tieredStorageConsumerClient != null;
    }

    private void checkUnavailability() {
        assert Thread.holdsLock(inputChannelsWithData);

        if (inputChannelsWithData.isEmpty()) {
            availabilityHelper.resetUnavailable();
        }
    }

    private BufferOrEvent transformToBufferOrEvent(
            Buffer buffer,
            boolean moreAvailable,
            InputChannel currentChannel,
            boolean morePriorityEvents)
            throws IOException, InterruptedException {
        if (buffer.isBuffer()) {
            //变换缓冲区
            return transformBuffer(buffer, moreAvailable, currentChannel, morePriorityEvents);
        } else {
            //变换事件
            return transformEvent(buffer, moreAvailable, currentChannel, morePriorityEvents);
        }
    }

    private BufferOrEvent transformBuffer(
            Buffer buffer,
            boolean moreAvailable,
            InputChannel currentChannel,
            boolean morePriorityEvents) {
        return new BufferOrEvent(
                //如果需要，解压缩缓冲区
                decompressBufferIfNeeded(buffer),
                currentChannel.getChannelInfo(),
                moreAvailable,
                morePriorityEvents);
    }

    private BufferOrEvent transformEvent(
            Buffer buffer,
            boolean moreAvailable,
            InputChannel currentChannel,
            boolean morePriorityEvents)
            throws IOException, InterruptedException {
        final AbstractEvent event;
        try {
            event = EventSerializer.fromBuffer(buffer, getClass().getClassLoader());
        } finally {
            //释放此缓冲区一次，即，如果引用计数达到0 ，则减少引用计数并回收缓冲区。
            buffer.recycleBuffer();
        }

        if (event.getClass() == EndOfPartitionEvent.class) {
            synchronized (inputChannelsWithData) {
                checkState(!channelsWithEndOfPartitionEvents.get(currentChannel.getChannelIndex()));
                channelsWithEndOfPartitionEvents.set(currentChannel.getChannelIndex());
                hasReceivedAllEndOfPartitionEvents =
                        channelsWithEndOfPartitionEvents.cardinality() == numberOfInputChannels;

                enqueuedInputChannelsWithData.clear(currentChannel.getChannelIndex());
                if (inputChannelsWithData.contains(currentChannel)) {
                    inputChannelsWithData.getAndRemove(channel -> channel == currentChannel);
                }
            }
            if (hasReceivedAllEndOfPartitionEvents) {
                // Because of race condition between:
                // 1. releasing inputChannelsWithData lock in this method and reaching this place
                // 2. empty data notification that re-enqueues a channel we can end up with
                // moreAvailable flag set to true, while we expect no more data.
                //由于以下条件之间的竞争条件：
                // 1. 在此方法中释放 inputChannelsWithData 锁并到达此位置
                // 2. 重新排队通道的空数据通知，我们最终可能会将 moreAvailable 标志设置为 true，而我们预计不会有更多数据。
                checkState(!moreAvailable || !pollNext().isPresent());
                moreAvailable = false;
                markAvailable();
            }

            currentChannel.releaseAllResources();
        } else if (event.getClass() == EndOfData.class) {
            synchronized (inputChannelsWithData) {
                checkState(!channelsWithEndOfUserRecords.get(currentChannel.getChannelIndex()));
                channelsWithEndOfUserRecords.set(currentChannel.getChannelIndex());
                hasReceivedEndOfData =
                        channelsWithEndOfUserRecords.cardinality() == numberOfInputChannels;
                shouldDrainOnEndOfData &= ((EndOfData) event).getStopMode() == StopMode.DRAIN;
            }
        }

        return new BufferOrEvent(
                event,
                buffer.getDataType().hasPriority(),
                currentChannel.getChannelInfo(),
                moreAvailable,
                buffer.getSize(),
                morePriorityEvents);
    }

    private Buffer decompressBufferIfNeeded(Buffer buffer) {
        if (buffer.isCompressed()) {
            try {
                checkNotNull(bufferDecompressor, "Buffer decompressor not set.");
                return bufferDecompressor.decompressToIntermediateBuffer(buffer);
            } finally {
                //释放此缓冲区一次，即，如果引用计数达到0 ，则减少引用计数并回收缓冲区。
                buffer.recycleBuffer();
            }
        }
        return buffer;
    }

    private void markAvailable() {
        CompletableFuture<?> toNotify;
        synchronized (inputChannelsWithData) {
            toNotify = availabilityHelper.getUnavailableToResetAvailable();
        }
        toNotify.complete(null);
    }

    @Override
    public void sendTaskEvent(TaskEvent event) throws IOException {
        synchronized (requestLock) {
            for (InputChannel inputChannel : inputChannels()) {
                inputChannel.sendTaskEvent(event);
            }

            if (numberOfUninitializedChannels > 0) {
                pendingEvents.add(event);
            }
        }
    }

    @Override
    public void resumeConsumption(InputChannelInfo channelInfo) throws IOException {
        checkState(!isFinished(), "InputGate already finished.");
        // BEWARE: consumption resumption only happens for streaming jobs in which all slots
        // are allocated together so there should be no UnknownInputChannel. As a result, it
        // is safe to not synchronize the requestLock here. We will refactor the code to not
        // rely on this assumption in the future.
        //注意：消耗恢复仅发生在所有槽都分配在一起的流作业中，因此不应存在 UnknownInputChannel。
        //因此，这里不同步requestLock是安全的。我们将重构代码，以便将来不再依赖这个假设。
        channels[channelInfo.getInputChannelIdx()].resumeConsumption();
    }

    @Override
    public void acknowledgeAllRecordsProcessed(InputChannelInfo channelInfo) throws IOException {
        checkState(!isFinished(), "InputGate already finished.");
        if (!enabledTieredStorage()) {
            channels[channelInfo.getInputChannelIdx()].acknowledgeAllRecordsProcessed();
        }
    }

    // ------------------------------------------------------------------------
    // Channel notifications
    // ------------------------------------------------------------------------

    void notifyChannelNonEmpty(InputChannel channel) {
        if (enabledTieredStorage()) {
            TieredStorageConsumerSpec tieredStorageConsumerSpec =
                    checkNotNull(tieredStorageConsumerSpecs).get(channel.getChannelIndex());
            checkNotNull(availabilityNotifier)
                    .notifyAvailable(
                            tieredStorageConsumerSpec.getPartitionId(),
                            tieredStorageConsumerSpec.getInputChannelId());
        } else {
            //RemoteInputChannel会将自己重新入队到InputGate.inputChannelsWithData队列中。
            queueChannel(checkNotNull(channel), null, false);
        }
    }

    /**
     * Notifies that the respective channel has a priority event at the head for the given buffer
     * number.
     *
     * <p>The buffer number limits the notification to the respective buffer and voids the whole
     * notification in case that the buffer has been polled in the meantime. That is, if task thread
     * polls the enqueued priority buffer before this notification occurs (notification is not
     * performed under lock), this buffer number allows {@link #queueChannel(InputChannel, Integer,
     * boolean)} to avoid spurious priority wake-ups.
     */
    //通知相应通道在给定缓冲区编号的头部有一个优先级事件。
    //缓冲区编号将通知限制到相应的缓冲区，并在同时轮询缓冲区的情况下使整个通知无效。
    //也就是说，如果任务线程在此通知发生之前轮询排队优先级缓冲区（通知不在锁定状态下执行），
    //则此缓冲区编号允许queueChannel(InputChannel, Integer, boolean)避免虚假优先级唤醒。
    void notifyPriorityEvent(InputChannel inputChannel, int prioritySequenceNumber) {
        queueChannel(checkNotNull(inputChannel), prioritySequenceNumber, false);
    }

    void notifyPriorityEventForce(InputChannel inputChannel) {
        queueChannel(checkNotNull(inputChannel), null, true);
    }

    void triggerPartitionStateCheck(
            ResultPartitionID partitionId, InputChannelInfo inputChannelInfo) {
        partitionProducerStateProvider.requestPartitionProducerState(
                consumedResultId,
                partitionId,
                ((PartitionProducerStateProvider.ResponseHandle responseHandle) -> {
                    boolean isProducingState =
                            new RemoteChannelStateChecker(partitionId, owningTaskName)
                                    .isProducerReadyOrAbortConsumption(responseHandle);
                    if (isProducingState) {
                        try {
                            retriggerPartitionRequest(
                                    partitionId.getPartitionId(), inputChannelInfo);
                        } catch (IOException t) {
                            responseHandle.failConsumption(t);
                        }
                    }
                }));
    }

    private void queueChannel(
            InputChannel channel, @Nullable Integer prioritySequenceNumber, boolean forcePriority) {
        //创建GateNotificationHelper
        try (GateNotificationHelper notification =
                new GateNotificationHelper(this, inputChannelsWithData)) {
            synchronized (inputChannelsWithData) {
                boolean priority = prioritySequenceNumber != null || forcePriority;

                if (!forcePriority
                        && priority
                        && isOutdated(
                                prioritySequenceNumber,
                                lastPrioritySequenceNumber[channel.getChannelIndex()])) {
                    // priority event at the given offset already polled (notification is not atomic
                    // in respect to
                    // buffer enqueuing), so just ignore the notification
                    //给定偏移处的优先级事件已轮询（通知对于缓冲区排队而言不是原子的），因此只需忽略该通知
                    return;
                }

                //添加到通道中
                if (!queueChannelUnsafe(channel, priority)) {
                    return;
                }

                if (priority && inputChannelsWithData.getNumPriorityElements() == 1) {
                    notification.notifyPriority();
                }
                if (inputChannelsWithData.size() == 1) {
                    //通知可用数据
                    notification.notifyDataAvailable();
                }
            }
        }
    }

    private boolean isOutdated(int sequenceNumber, int lastSequenceNumber) {
        if ((lastSequenceNumber < 0) != (sequenceNumber < 0)
                && Math.max(lastSequenceNumber, sequenceNumber) > Integer.MAX_VALUE / 2) {
            // probably overflow of one of the two numbers, the negative one is greater then
            // 可能两个数之一溢出，负数大于
            return lastSequenceNumber < 0;
        }
        return lastSequenceNumber >= sequenceNumber;
    }

    /**
     * Queues the channel if not already enqueued and not received EndOfPartition, potentially
     * raising the priority.
     *
     * @return true iff it has been enqueued/prioritized = some change to {@link
     *     #inputChannelsWithData} happened
     */
    //如果尚未排队并且未接收到EndOfPartition，则将通道排队，这可能会提高优先级。
    private boolean queueChannelUnsafe(InputChannel channel, boolean priority) {
        assert Thread.holdsLock(inputChannelsWithData);
        if (channelsWithEndOfPartitionEvents.get(channel.getChannelIndex())) {
            return false;
        }

        final boolean alreadyEnqueued =
                enqueuedInputChannelsWithData.get(channel.getChannelIndex());
        if (alreadyEnqueued
                && (!priority || inputChannelsWithData.containsPriorityElement(channel))) {
            // already notified / prioritized (double notification), ignore
            // 已通知优先（双重通知），忽略
            return false;
        }

        //添加
        inputChannelsWithData.add(channel, priority, alreadyEnqueued);
        if (!alreadyEnqueued) {
            enqueuedInputChannelsWithData.set(channel.getChannelIndex());
        }
        return true;
    }

    private Optional<InputChannel> getChannel(boolean blocking) throws InterruptedException {
        assert Thread.holdsLock(inputChannelsWithData);

        while (inputChannelsWithData.isEmpty()) {
            if (closeFuture.isDone()) {
                throw new IllegalStateException("Released");
            }

            if (blocking) {
                inputChannelsWithData.wait();
            } else {
                availabilityHelper.resetUnavailable();
                return Optional.empty();
            }
        }

        InputChannel inputChannel = inputChannelsWithData.poll();
        enqueuedInputChannelsWithData.clear(inputChannel.getChannelIndex());

        return Optional.of(inputChannel);
    }

    private void setupTieredStorageNettyService(
            TieredStorageNettyServiceImpl nettyService,
            List<TieredStorageConsumerSpec> tieredStorageConsumerSpecs) {
        List<Supplier<InputChannel>> channelSuppliers = new ArrayList<>();
        for (int index = 0; index < channels.length; ++index) {
            int channelIndex = index;
            channelSuppliers.add(() -> channels[channelIndex]);
        }
        nettyService.setupInputChannels(tieredStorageConsumerSpecs, channelSuppliers);
    }

    /** The default implementation of {@link AvailabilityNotifier}. */
    //AvailabilityNotifier的默认实现。
    private class AvailabilityNotifierImpl implements AvailabilityNotifier {

        private AvailabilityNotifierImpl() {}

        @Override
        public void notifyAvailable(
                TieredStoragePartitionId partitionId, TieredStorageInputChannelId inputChannelId) {
            Map<InputChannelInfo, InputChannel> channels =
                    inputChannels.get(partitionId.getPartitionID().getPartitionId());
            if (channels == null) {
                return;
            }
            InputChannelInfo inputChannelInfo =
                    new InputChannelInfo(gateIndex, inputChannelId.getInputChannelId());
            InputChannel inputChannel = channels.get(inputChannelInfo);
            if (inputChannel != null) {
                queueChannel(inputChannel, null, false);
            }
        }
    }

    // ------------------------------------------------------------------------

    @VisibleForTesting
    public Map<Tuple2<IntermediateResultPartitionID, InputChannelInfo>, InputChannel>
            getInputChannels() {
        Map<Tuple2<IntermediateResultPartitionID, InputChannelInfo>, InputChannel> result =
                new HashMap<>();
        for (Map.Entry<IntermediateResultPartitionID, Map<InputChannelInfo, InputChannel>>
                mapEntry : inputChannels.entrySet()) {
            for (Map.Entry<InputChannelInfo, InputChannel> entry : mapEntry.getValue().entrySet()) {
                result.put(Tuple2.of(mapEntry.getKey(), entry.getKey()), entry.getValue());
            }
        }
        return result;
    }

    public Iterable<InputChannel> inputChannels() {
        return () ->
                new Iterator<InputChannel>() {
                    private final Iterator<Map<InputChannelInfo, InputChannel>> mapIterator =
                            inputChannels.values().iterator();

                    private Iterator<InputChannel> iterator = null;

                    @Override
                    public boolean hasNext() {
                        return (iterator != null && iterator.hasNext()) || mapIterator.hasNext();
                    }

                    @Override
                    public InputChannel next() {
                        if ((iterator == null || !iterator.hasNext()) && mapIterator.hasNext()) {
                            iterator = mapIterator.next().values().iterator();
                        }

                        if (iterator == null || !iterator.hasNext()) {
                            return null;
                        }

                        return iterator.next();
                    }
                };
    }
}
