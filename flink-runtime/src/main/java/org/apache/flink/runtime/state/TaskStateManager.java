/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.PrioritizedOperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.SubTaskInitializationMetrics;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.checkpoint.channel.SequentialChannelStateReader;
import org.apache.flink.runtime.checkpoint.filemerging.FileMergingSnapshotManager;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.changelog.ChangelogStateHandle;
import org.apache.flink.runtime.state.changelog.StateChangelogStorage;
import org.apache.flink.runtime.state.changelog.StateChangelogStorageView;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Optional;

/**
 * This interface provides methods to report and retrieve state for a task.
 *
 * <p>When a checkpoint or savepoint is triggered on a task, it will create snapshots for all stream
 * operator instances it owns. All operator snapshots from the task are then reported via this
 * interface. A typical implementation will dispatch and forward the reported state information to
 * interested parties such as the checkpoint coordinator or a local state store.
 *
 * <p>This interface also offers the complementary method that provides access to previously saved
 * state of operator instances in the task for restore purposes.
 */
public interface TaskStateManager extends CheckpointListener, AutoCloseable {

    void reportInitializationMetrics(SubTaskInitializationMetrics subTaskInitializationMetrics);

    /**
     * Report the state snapshots for the operator instances running in the owning task.
     *
     * @param checkpointMetaData meta data from the checkpoint request.
     * @param checkpointMetrics task level metrics for the checkpoint.
     * @param acknowledgedState the reported states to acknowledge to the job manager.
     * @param localState the reported states for local recovery.
     */
    //报告在所属任务中运行的操作员实例的状态快照。
    void reportTaskStateSnapshots(
            @Nonnull CheckpointMetaData checkpointMetaData,
            @Nonnull CheckpointMetrics checkpointMetrics,
            @Nullable TaskStateSnapshot acknowledgedState,
            @Nullable TaskStateSnapshot localState);

    InflightDataRescalingDescriptor getInputRescalingDescriptor();

    InflightDataRescalingDescriptor getOutputRescalingDescriptor();

    /**
     * Report the stats for state snapshots for an aborted checkpoint.
     *
     * @param checkpointMetaData meta data from the checkpoint request.
     * @param checkpointMetrics task level metrics for the checkpoint.
     */
    void reportIncompleteTaskStateSnapshots(
            CheckpointMetaData checkpointMetaData, CheckpointMetrics checkpointMetrics);

    /** Whether all the operators of the task are finished on restore. */
    boolean isTaskDeployedAsFinished();

    /** Acquires the checkpoint id to restore from. */
    Optional<Long> getRestoreCheckpointId();

    /**
     * Returns means to restore previously reported state of an operator running in the owning task.
     *
     * @param operatorID the id of the operator for which we request state.
     * @return Previous state for the operator. The previous state can be empty if the operator had
     *     no previous state.
     */
    @Nonnull
    PrioritizedOperatorSubtaskState prioritizedOperatorState(OperatorID operatorID);

    /**
     * Get the restored state from jobManager which belongs to an operator running in the owning
     * task.
     *
     * @param operatorID the id of the operator for which we request state.
     * @return the subtask restored state from jobManager.
     */
    Optional<OperatorSubtaskState> getSubtaskJobManagerRestoredState(OperatorID operatorID);

    /**
     * Returns the configuration for local recovery, i.e. the base directories for all file-based
     * local state of the owning subtask and the general mode for local recovery.
     */
    @Nonnull
    LocalRecoveryConfig createLocalRecoveryConfig();

    SequentialChannelStateReader getSequentialChannelStateReader();

    /** Returns the configured state changelog storage for this task. */
    @Nullable
    StateChangelogStorage<?> getStateChangelogStorage();

    /**
     * Returns the state changelog storage view of given {@link ChangelogStateHandle} for this task.
     */
    @Nullable
    StateChangelogStorageView<?> getStateChangelogStorageView(
            Configuration configuration, ChangelogStateHandle changelogStateHandle);

    @Nullable
    FileMergingSnapshotManager getFileMergingSnapshotManager();
}
