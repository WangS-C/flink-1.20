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

package org.apache.flink.runtime.jobgraph.tasks;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Objects;

/**
 * Configuration settings for the {@link CheckpointCoordinator}. This includes the checkpoint
 * interval, the checkpoint timeout, the pause between checkpoints, the maximum number of concurrent
 * checkpoints and settings for externalized checkpoints.
 */
//CheckpointCoordinator的配置设置。
//这包括检查点间隔、检查点超时、检查点之间的暂停、并发检查点的最大数量以及外部化检查点的设置。
public class CheckpointCoordinatorConfiguration implements Serializable {

    //最短检查点时间
    public static final long MINIMAL_CHECKPOINT_TIME = 10;

    // interval of max value means disable periodic checkpoint
    //最大值间隔表示禁用周期性检查点
    public static final long DISABLED_CHECKPOINT_INTERVAL = Long.MAX_VALUE;

    private static final long serialVersionUID = 2L;

    //检查点间隔
    private final long checkpointInterval;

    //积压期间的检查点间隔
    private final long checkpointIntervalDuringBacklog;

    //检查点超时
    private final long checkpointTimeout;

    //检查点之间的最小暂停时间
    private final long minPauseBetweenCheckpoints;

    //最大并发检查点数
    private final int maxConcurrentCheckpoints;

    //可容忍的检查点故障数
    private final int tolerableCheckpointFailureNumber;

    /** Settings for what to do with checkpoints when a job finishes. */
    //设置作业完成后如何处理检查点。
    //检查点保留策略
    private final CheckpointRetentionPolicy checkpointRetentionPolicy;

    /**
     * Flag indicating whether exactly once checkpoint mode has been configured. If <code>false
     * </code>, at least once mode has been configured. This is not a necessary attribute, because
     * the checkpointing mode is only relevant for the stream tasks, but we expose it here to
     * forward it to the web runtime UI.
     */
    //指示是否已配置恰好一次检查点模式的标志。
    //如果为false ，则至少配置了一次模式。这不是必需的属性，因为检查点模式仅与流任务相关，
    //但我们在此处公开它以将其转发到 Web 运行时 UI。
    private final boolean isExactlyOnce;

    //是否启用未对齐检查点
    private final boolean isUnalignedCheckpointsEnabled;

    //对齐检查点超时
    private final long alignedCheckpointTimeout;

    private final long checkpointIdOfIgnoredInFlightData;

    //任务完成后启用检查点
    private final boolean enableCheckpointsAfterTasksFinish;

    /** @deprecated use {@link #builder()}. */
    @Deprecated
    @VisibleForTesting
    public CheckpointCoordinatorConfiguration(
            long checkpointInterval,
            long checkpointTimeout,
            long minPauseBetweenCheckpoints,
            int maxConcurrentCheckpoints,
            CheckpointRetentionPolicy checkpointRetentionPolicy,
            boolean isExactlyOnce,
            boolean isUnalignedCheckpoint,
            int tolerableCpFailureNumber,
            long checkpointIdOfIgnoredInFlightData) {
        this(
                checkpointInterval,
                checkpointInterval,
                checkpointTimeout,
                minPauseBetweenCheckpoints,
                maxConcurrentCheckpoints,
                checkpointRetentionPolicy,
                isExactlyOnce,
                tolerableCpFailureNumber,
                isUnalignedCheckpoint,
                0,
                checkpointIdOfIgnoredInFlightData,
                false);
    }

    private CheckpointCoordinatorConfiguration(
            long checkpointInterval,
            long checkpointIntervalDuringBacklog,
            long checkpointTimeout,
            long minPauseBetweenCheckpoints,
            int maxConcurrentCheckpoints,
            CheckpointRetentionPolicy checkpointRetentionPolicy,
            boolean isExactlyOnce,
            int tolerableCpFailureNumber,
            boolean isUnalignedCheckpointsEnabled,
            long alignedCheckpointTimeout,
            long checkpointIdOfIgnoredInFlightData,
            boolean enableCheckpointsAfterTasksFinish) {

        if (checkpointIntervalDuringBacklog < MINIMAL_CHECKPOINT_TIME) {
            // interval of max value means disable periodic checkpoint
            checkpointIntervalDuringBacklog = DISABLED_CHECKPOINT_INTERVAL;
        }

        // sanity checks
        if (checkpointInterval < MINIMAL_CHECKPOINT_TIME
                || checkpointTimeout < MINIMAL_CHECKPOINT_TIME
                || minPauseBetweenCheckpoints < 0
                || maxConcurrentCheckpoints < 1
                || tolerableCpFailureNumber < 0) {
            throw new IllegalArgumentException();
        }
        Preconditions.checkArgument(
                !isUnalignedCheckpointsEnabled || maxConcurrentCheckpoints <= 1,
                "maxConcurrentCheckpoints can't be > 1 if UnalignedCheckpoints enabled");

        this.checkpointInterval = checkpointInterval;
        this.checkpointIntervalDuringBacklog = checkpointIntervalDuringBacklog;
        this.checkpointTimeout = checkpointTimeout;
        this.minPauseBetweenCheckpoints = minPauseBetweenCheckpoints;
        this.maxConcurrentCheckpoints = maxConcurrentCheckpoints;
        this.checkpointRetentionPolicy = Preconditions.checkNotNull(checkpointRetentionPolicy);
        this.isExactlyOnce = isExactlyOnce;
        this.tolerableCheckpointFailureNumber = tolerableCpFailureNumber;
        this.isUnalignedCheckpointsEnabled = isUnalignedCheckpointsEnabled;
        this.alignedCheckpointTimeout = alignedCheckpointTimeout;
        this.checkpointIdOfIgnoredInFlightData = checkpointIdOfIgnoredInFlightData;
        this.enableCheckpointsAfterTasksFinish = enableCheckpointsAfterTasksFinish;
    }

    public long getCheckpointInterval() {
        return checkpointInterval;
    }

    public boolean isCheckpointingEnabled() {
        return checkpointInterval > 0 && checkpointInterval < DISABLED_CHECKPOINT_INTERVAL;
    }

    public long getCheckpointIntervalDuringBacklog() {
        return checkpointIntervalDuringBacklog;
    }

    public long getCheckpointTimeout() {
        return checkpointTimeout;
    }

    public long getMinPauseBetweenCheckpoints() {
        return minPauseBetweenCheckpoints;
    }

    public int getMaxConcurrentCheckpoints() {
        return maxConcurrentCheckpoints;
    }

    public CheckpointRetentionPolicy getCheckpointRetentionPolicy() {
        return checkpointRetentionPolicy;
    }

    public boolean isExactlyOnce() {
        return isExactlyOnce;
    }

    public int getTolerableCheckpointFailureNumber() {
        return tolerableCheckpointFailureNumber;
    }

    public boolean isUnalignedCheckpointsEnabled() {
        return isUnalignedCheckpointsEnabled;
    }

    public long getAlignedCheckpointTimeout() {
        return alignedCheckpointTimeout;
    }

    public long getCheckpointIdOfIgnoredInFlightData() {
        return checkpointIdOfIgnoredInFlightData;
    }

    public boolean isEnableCheckpointsAfterTasksFinish() {
        return enableCheckpointsAfterTasksFinish;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CheckpointCoordinatorConfiguration that = (CheckpointCoordinatorConfiguration) o;
        return checkpointInterval == that.checkpointInterval
                && checkpointTimeout == that.checkpointTimeout
                && minPauseBetweenCheckpoints == that.minPauseBetweenCheckpoints
                && maxConcurrentCheckpoints == that.maxConcurrentCheckpoints
                && isExactlyOnce == that.isExactlyOnce
                && isUnalignedCheckpointsEnabled == that.isUnalignedCheckpointsEnabled
                && alignedCheckpointTimeout == that.alignedCheckpointTimeout
                && checkpointRetentionPolicy == that.checkpointRetentionPolicy
                && tolerableCheckpointFailureNumber == that.tolerableCheckpointFailureNumber
                && checkpointIdOfIgnoredInFlightData == that.checkpointIdOfIgnoredInFlightData
                && enableCheckpointsAfterTasksFinish == that.enableCheckpointsAfterTasksFinish;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                checkpointInterval,
                checkpointTimeout,
                minPauseBetweenCheckpoints,
                maxConcurrentCheckpoints,
                checkpointRetentionPolicy,
                isExactlyOnce,
                isUnalignedCheckpointsEnabled,
                alignedCheckpointTimeout,
                tolerableCheckpointFailureNumber,
                checkpointIdOfIgnoredInFlightData,
                enableCheckpointsAfterTasksFinish);
    }

    @Override
    public String toString() {
        return "JobCheckpointingConfiguration{"
                + "checkpointInterval="
                + checkpointInterval
                + ", checkpointTimeout="
                + checkpointTimeout
                + ", minPauseBetweenCheckpoints="
                + minPauseBetweenCheckpoints
                + ", maxConcurrentCheckpoints="
                + maxConcurrentCheckpoints
                + ", checkpointRetentionPolicy="
                + checkpointRetentionPolicy
                + ", isExactlyOnce="
                + isExactlyOnce
                + ", isUnalignedCheckpoint="
                + isUnalignedCheckpointsEnabled
                + ", alignedCheckpointTimeout="
                + alignedCheckpointTimeout
                + ", tolerableCheckpointFailureNumber="
                + tolerableCheckpointFailureNumber
                + ", checkpointIdOfIgnoredInFlightData="
                + checkpointIdOfIgnoredInFlightData
                + ", enableCheckpointsAfterTasksFinish="
                + enableCheckpointsAfterTasksFinish
                + '}';
    }

    public static CheckpointCoordinatorConfigurationBuilder builder() {
        return new CheckpointCoordinatorConfigurationBuilder();
    }

    /** {@link CheckpointCoordinatorConfiguration} builder. */
    public static class CheckpointCoordinatorConfigurationBuilder {
        private long checkpointInterval = MINIMAL_CHECKPOINT_TIME;
        private long checkpointIntervalDuringBacklog = MINIMAL_CHECKPOINT_TIME;
        private long checkpointTimeout = MINIMAL_CHECKPOINT_TIME;
        private long minPauseBetweenCheckpoints;
        private int maxConcurrentCheckpoints = 1;
        private CheckpointRetentionPolicy checkpointRetentionPolicy =
                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION;
        private boolean isExactlyOnce = true;
        private int tolerableCheckpointFailureNumber;
        private boolean isUnalignedCheckpointsEnabled;
        private long alignedCheckpointTimeout = 0;
        private long checkpointIdOfIgnoredInFlightData;
        private boolean enableCheckpointsAfterTasksFinish;

        public CheckpointCoordinatorConfiguration build() {
            return new CheckpointCoordinatorConfiguration(
                    checkpointInterval,
                    checkpointIntervalDuringBacklog,
                    checkpointTimeout,
                    minPauseBetweenCheckpoints,
                    maxConcurrentCheckpoints,
                    checkpointRetentionPolicy,
                    isExactlyOnce,
                    tolerableCheckpointFailureNumber,
                    isUnalignedCheckpointsEnabled,
                    alignedCheckpointTimeout,
                    checkpointIdOfIgnoredInFlightData,
                    enableCheckpointsAfterTasksFinish);
        }

        public CheckpointCoordinatorConfigurationBuilder setCheckpointInterval(
                long checkpointInterval) {
            this.checkpointInterval = checkpointInterval;
            return this;
        }

        public CheckpointCoordinatorConfigurationBuilder setCheckpointIntervalDuringBacklog(
                long checkpointIntervalDuringBacklog) {
            this.checkpointIntervalDuringBacklog = checkpointIntervalDuringBacklog;
            return this;
        }

        public CheckpointCoordinatorConfigurationBuilder setCheckpointTimeout(
                long checkpointTimeout) {
            this.checkpointTimeout = checkpointTimeout;
            return this;
        }

        public CheckpointCoordinatorConfigurationBuilder setMinPauseBetweenCheckpoints(
                long minPauseBetweenCheckpoints) {
            this.minPauseBetweenCheckpoints = minPauseBetweenCheckpoints;
            return this;
        }

        public CheckpointCoordinatorConfigurationBuilder setMaxConcurrentCheckpoints(
                int maxConcurrentCheckpoints) {
            this.maxConcurrentCheckpoints = maxConcurrentCheckpoints;
            return this;
        }

        public CheckpointCoordinatorConfigurationBuilder setCheckpointRetentionPolicy(
                CheckpointRetentionPolicy checkpointRetentionPolicy) {
            this.checkpointRetentionPolicy = checkpointRetentionPolicy;
            return this;
        }

        public CheckpointCoordinatorConfigurationBuilder setExactlyOnce(boolean exactlyOnce) {
            isExactlyOnce = exactlyOnce;
            return this;
        }

        public CheckpointCoordinatorConfigurationBuilder setTolerableCheckpointFailureNumber(
                int tolerableCheckpointFailureNumber) {
            this.tolerableCheckpointFailureNumber = tolerableCheckpointFailureNumber;
            return this;
        }

        public CheckpointCoordinatorConfigurationBuilder setUnalignedCheckpointsEnabled(
                boolean unalignedCheckpointsEnabled) {
            isUnalignedCheckpointsEnabled = unalignedCheckpointsEnabled;
            return this;
        }

        public CheckpointCoordinatorConfigurationBuilder setAlignedCheckpointTimeout(
                long alignedCheckpointTimeout) {
            this.alignedCheckpointTimeout = alignedCheckpointTimeout;
            return this;
        }

        public CheckpointCoordinatorConfigurationBuilder setCheckpointIdOfIgnoredInFlightData(
                long checkpointIdOfIgnoredInFlightData) {
            this.checkpointIdOfIgnoredInFlightData = checkpointIdOfIgnoredInFlightData;
            return this;
        }

        public CheckpointCoordinatorConfigurationBuilder setEnableCheckpointsAfterTasksFinish(
                boolean enableCheckpointsAfterTasksFinish) {
            this.enableCheckpointsAfterTasksFinish = enableCheckpointsAfterTasksFinish;
            return this;
        }
    }
}
