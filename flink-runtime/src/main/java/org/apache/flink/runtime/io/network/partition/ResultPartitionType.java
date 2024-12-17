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

/** Type of a result partition. */
public enum ResultPartitionType {

    /**
     * Blocking partitions represent blocking data exchanges, where the data stream is first fully
     * produced and then consumed. This is an option that is only applicable to bounded streams and
     * can be used in bounded stream runtime and recovery.
     *
     * <p>Blocking partitions can be consumed multiple times and concurrently.
     *
     * <p>The partition is not automatically released after being consumed (like for example the
     * {@link #PIPELINED} partitions), but only released through the scheduler, when it determines
     * that the partition is no longer needed.
     */
    //阻塞分区表示阻塞数据交换，其中数据流首先被完全生成，然后被消费。这个选项只适用于有界流，并且可以在有界流运行时和恢复中使用。
    //阻塞分区可以被多次并发地使用。
    //分区在被使用后不会自动释放(例如 管线式 分区)，但只有当调度程序确定不再需要该分区时才通过调度程序释放。
    BLOCKING(true, false, false, ConsumingConstraint.BLOCKING, ReleaseBy.SCHEDULER),

    /**
     * BLOCKING_PERSISTENT partitions are similar to {@link #BLOCKING} partitions, but have a
     * user-specified life cycle.
     *
     * <p>BLOCKING_PERSISTENT partitions are dropped upon explicit API calls to the JobManager or
     * ResourceManager, rather than by the scheduler.
     *
     * <p>Otherwise, the partition may only be dropped by safety-nets during failure handling
     * scenarios, like when the TaskManager exits or when the TaskManager loses connection to
     * JobManager / ResourceManager for too long.
     */
    //BLOCKING_PERSISTENT分区类似于 阻塞 分区，但具有用户指定的生命周期。
    //BLOCKING_PERSISTENT分区在对JobManager或ResourceManager的显式API调用时被删除，而不是由调度程序删除。
    //否则，只有当TaskManager退出或TaskManager与JobManager / ResourceManager长时间失去连接时，才会被安全网丢弃。
    BLOCKING_PERSISTENT(true, false, true, ConsumingConstraint.BLOCKING, ReleaseBy.SCHEDULER),

    /**
     * A pipelined streaming data exchange. This is applicable to both bounded and unbounded
     * streams.
     *
     * <p>Pipelined results can be consumed only once by a single consumer and are automatically
     * disposed when the stream has been consumed.
     *
     * <p>This result partition type may keep an arbitrary amount of data in-flight, in contrast to
     * the {@link #PIPELINED_BOUNDED} variant.
     */
    PIPELINED(false, false, false, ConsumingConstraint.MUST_BE_PIPELINED, ReleaseBy.UPSTREAM),

    /**
     * Pipelined partitions with a bounded (local) buffer pool.
     *
     * <p>For streaming jobs, a fixed limit on the buffer pool size should help avoid that too much
     * data is being buffered and checkpoint barriers are delayed. In contrast to limiting the
     * overall network buffer pool size, this, however, still allows to be flexible with regards to
     * the total number of partitions by selecting an appropriately big network buffer pool size.
     *
     * <p>For batch jobs, it will be best to keep this unlimited ({@link #PIPELINED}) since there
     * are no checkpoint barriers.
     */
    //带有有界(本地) 缓冲池的流水线分区。
    //对于流作业，对缓冲池大小的固定限制应该有助于避免缓冲过多的数据和检查点屏障延迟。然而，与限制总体网络缓冲池大小相反，
    // 这仍然允许通过选择适当的大网络缓冲池大小来灵活地处理分区的总数。
    //对于批处理作业，最好将其设置为无限制(管线式)，因为这里没有关卡障碍。
    PIPELINED_BOUNDED(
            false, true, false, ConsumingConstraint.MUST_BE_PIPELINED, ReleaseBy.UPSTREAM),

    /**
     * Pipelined partitions with a bounded (local) buffer pool to support downstream task to
     * continue consuming data after reconnection in Approximate Local-Recovery.
     *
     * <p>Pipelined results can be consumed only once by a single consumer at one time. {@link
     * #PIPELINED_APPROXIMATE} is different from {@link #PIPELINED} and {@link #PIPELINED_BOUNDED}
     * in that {@link #PIPELINED_APPROXIMATE} partition can be reconnected after down stream task
     * fails.
     */
    //带有有界(本地) 缓冲池的流水线分区，支持在近似本地恢复中重新连接后继续使用数据的下游任务。
    //流水线结果一次只能由单个消费者消费一次。 PIPELINED_APPROXIMATE 不同于 管线式 和 PIPELINED_BOUNDED 在那 PIPELINED_APPROXIMATE 下行任务失败后可以重新连接分区
    PIPELINED_APPROXIMATE(
            false, true, false, ConsumingConstraint.CAN_BE_PIPELINED, ReleaseBy.UPSTREAM),

    /**
     * Hybrid partitions with a bounded (local) buffer pool to support downstream task to
     * simultaneous reading and writing shuffle data.
     *
     * <p>Hybrid partitions can be consumed any time, whether fully produced or not.
     *
     * <p>HYBRID_FULL partitions is re-consumable, so double calculation can be avoided during
     * failover.
     */
    //具有有界 (本地) 缓冲池的混合分区，以支持下游任务同时读取和写入shuffle数据。
    //无论是否完全生产，混合分区都可以随时使用。
    //HYBRID_FULL分区是可重新消耗的，因此在故障转移期间可以避免双重计算。
    HYBRID_FULL(true, false, false, ConsumingConstraint.CAN_BE_PIPELINED, ReleaseBy.SCHEDULER),

    /**
     * HYBRID_SELECTIVE partitions are similar to {@link #HYBRID_FULL} partitions, but it is not
     * re-consumable.
     */
    // HYBRID_SELECTIVE分区与hybrid_full分区类似，但不可重复使用。
    HYBRID_SELECTIVE(
            false, false, false, ConsumingConstraint.CAN_BE_PIPELINED, ReleaseBy.SCHEDULER);

    /**
     * Can this result partition be consumed by multiple downstream consumers for multiple times.
     */
    //此结果分区是否可以被多个下游消费者多次使用
    private final boolean isReconsumable;

    /** Does this partition use a limited number of (network) buffers? */
    //此分区是否使用有限数量的 (网络) 缓冲区
    private final boolean isBounded;

    /** This partition will not be released after consuming if 'isPersistent' is true. */
    //如果为true，则在使用后不会释放此分区。
    private final boolean isPersistent;

    private final ConsumingConstraint consumingConstraint;

    private final ReleaseBy releaseBy;

    /** ConsumingConstraint indicates when can the downstream consume the upstream. */
    private enum ConsumingConstraint {
        /** Upstream must be finished before downstream consume. */
        //上游必须在下游消费之前完成。
        BLOCKING,
        /** Downstream can consume while upstream is running. */
        //下游可以在上游运行时消耗
        CAN_BE_PIPELINED,
        /** Downstream must consume while upstream is running. */
        //当上游运行时，下游必须消耗
        MUST_BE_PIPELINED
    }

    /**
     * ReleaseBy indicates who is responsible for releasing the result partition.
     *
     * <p>Attention: This may only be a short-term solution to deal with the partition release
     * logic. We can discuss the issue of unifying the partition release logic in FLINK-27948. Once
     * the ticket is resolved, we can remove the enumeration here.
     */
    //Releasebby指示谁负责释放结果分区。
    //注意: 这可能只是处理分区释放逻辑的短期解决方案。
    //我们可以讨论在FLINK-27948中统一分区释放逻辑的问题。
    //一旦解决了票证，我们就可以在这里删除枚举。
    private enum ReleaseBy {
        UPSTREAM,
        SCHEDULER
    }

    /** Specifies the behaviour of an intermediate result partition at runtime. */
    ResultPartitionType(
            boolean isReconsumable,
            boolean isBounded,
            boolean isPersistent,
            ConsumingConstraint consumingConstraint,
            ReleaseBy releaseBy) {
        this.isReconsumable = isReconsumable;
        this.isBounded = isBounded;
        this.isPersistent = isPersistent;
        this.consumingConstraint = consumingConstraint;
        this.releaseBy = releaseBy;
    }

    /** return if this partition's upstream and downstream must be scheduled in the same time. */
    public boolean mustBePipelinedConsumed() {
        return consumingConstraint == ConsumingConstraint.MUST_BE_PIPELINED;
    }

    /** return if this partition's upstream and downstream support scheduling in the same time. */
    public boolean canBePipelinedConsumed() {
        return consumingConstraint == ConsumingConstraint.CAN_BE_PIPELINED
                || consumingConstraint == ConsumingConstraint.MUST_BE_PIPELINED;
    }

    public boolean isReleaseByScheduler() {
        return releaseBy == ReleaseBy.SCHEDULER;
    }

    public boolean isReleaseByUpstream() {
        return releaseBy == ReleaseBy.UPSTREAM;
    }

    /**
     * {@link #isBlockingOrBlockingPersistentResultPartition()} is used to judge whether it is the
     * specified {@link #BLOCKING} or {@link #BLOCKING_PERSISTENT} resultPartitionType.
     *
     * <p>this method suitable for judgment conditions related to the specific implementation of
     * {@link ResultPartitionType}.
     *
     * <p>this method not related to data consumption and partition release. As for the logic
     * related to partition release, use {@link #isReleaseByScheduler()} instead, and as consume
     * type, use {@link #mustBePipelinedConsumed()} or {@link #canBePipelinedConsumed()} instead.
     */
    //isBlockingOrBlockingPersistentResultPartition()用于判断是否是指定的BLOCKING或BLOCKING_PERSISTENT resultPartitionType。
    //该方法适用于ResultPartitionType具体实现相关的判断条件。
    //该方法与数据消耗和分区释放无关。
    // 对于分区释放相关的逻辑，使用isReleaseByScheduler()代替，
    // 消费类型使用mustBePipelinedConsumed()或canBePipelinedConsumed()代替。
    public boolean isBlockingOrBlockingPersistentResultPartition() {
        return this == BLOCKING || this == BLOCKING_PERSISTENT;
    }

    /**
     * {@link #isHybridResultPartition()} is used to judge whether it is the specified {@link
     * #HYBRID_FULL} or {@link #HYBRID_SELECTIVE} resultPartitionType.
     *
     * <p>this method suitable for judgment conditions related to the specific implementation of
     * {@link ResultPartitionType}.
     *
     * <p>this method not related to data consumption and partition release. As for the logic
     * related to partition release, use {@link #isReleaseByScheduler()} instead, and as consume
     * type, use {@link #mustBePipelinedConsumed()} or {@link #canBePipelinedConsumed()} instead.
     */
    public boolean isHybridResultPartition() {
        return this == HYBRID_FULL || this == HYBRID_SELECTIVE;
    }

    /**
     * {@link #isPipelinedOrPipelinedBoundedResultPartition()} is used to judge whether it is the
     * specified {@link #PIPELINED} or {@link #PIPELINED_BOUNDED} resultPartitionType.
     *
     * <p>This method suitable for judgment conditions related to the specific implementation of
     * {@link ResultPartitionType}.
     *
     * <p>This method not related to data consumption and partition release. As for the logic
     * related to partition release, use {@link #isReleaseByScheduler()} instead, and as consume
     * type, use {@link #mustBePipelinedConsumed()} or {@link #canBePipelinedConsumed()} instead.
     */
    //isPipelinedOrPipelinedBoundedResultPartition()用于判断是否是指定的PIPELINED或PIPELINED_BOUNDED resultPartitionType。
    //该方法适用于ResultPartitionType具体实现相关的判断条件。
    //该方法与数据消耗和分区释放无关。
    //对于分区释放相关的逻辑，使用isReleaseByScheduler()代替，
    //消费类型使用mustBePipelinedConsumed()或canBePipelinedConsumed()代替。
    public boolean isPipelinedOrPipelinedBoundedResultPartition() {
        return this == PIPELINED || this == PIPELINED_BOUNDED;
    }

    /**
     * Whether this partition uses a limited number of (network) buffers or not.
     *
     * @return <tt>true</tt> if the number of buffers should be bound to some limit
     */
    public boolean isBounded() {
        return isBounded;
    }

    public boolean isPersistent() {
        return isPersistent;
    }

    public boolean supportCompression() {
        return isBlockingOrBlockingPersistentResultPartition()
                || this == HYBRID_FULL
                || this == HYBRID_SELECTIVE;
    }

    public boolean isReconsumable() {
        return isReconsumable;
    }
}
