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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.failover.ResultPartitionAvailabilityChecker;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.query.KvStateLocationRegistry;
import org.apache.flink.runtime.scheduler.InternalFailuresListener;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.util.OptionalFailure;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * The execution graph is the central data structure that coordinates the distributed execution of a
 * data flow. It keeps representations of each parallel task, each intermediate stream, and the
 * communication between them.
 *
 * <p>The execution graph consists of the following constructs:
 *
 * <ul>
 *   <li>The {@link ExecutionJobVertex} represents one vertex from the JobGraph (usually one
 *       operation like "map" or "join") during execution. It holds the aggregated state of all
 *       parallel subtasks. The ExecutionJobVertex is identified inside the graph by the {@link
 *       JobVertexID}, which it takes from the JobGraph's corresponding JobVertex.
 *   <li>The {@link ExecutionVertex} represents one parallel subtask. For each ExecutionJobVertex,
 *       there are as many ExecutionVertices as the parallelism. The ExecutionVertex is identified
 *       by the ExecutionJobVertex and the index of the parallel subtask
 *   <li>The {@link Execution} is one attempt to execute a ExecutionVertex. There may be multiple
 *       Executions for the ExecutionVertex, in case of a failure, or in the case where some data
 *       needs to be recomputed because it is no longer available when requested by later
 *       operations. An Execution is always identified by an {@link ExecutionAttemptID}. All
 *       messages between the JobManager and the TaskManager about deployment of tasks and updates
 *       in the task status always use the ExecutionAttemptID to address the message receiver.
 * </ul>
 */
//执行图是协调数据流分布式执行的中央数据结构。它保存每个并行任务、每个中间流以及它们之间的通信的表示。
//执行图由以下结构组成：
//ExecutionJobVertex表示执行期间 JobGraph 中的一个顶点（通常是一个操作，如“map”或“join”）。它保存所有并行子任务的聚合状态。ExecutionJobVertex 在图中通过JobVertexID进行标识，该标识取自 JobGraph 的相应 JobVertex。
//ExecutionVertex表示一个并行子任务。对于每个 ExecutionJobVertex，有与并行度相同的 ExecutionVertices。ExecutionVertex 由 ExecutionJobVertex 和并行子任务的索引标识
//Execution是执行 ExecutionVertex 的一次尝试。如果发生故障，或者某些数据在后续操作请求时不再可用，则 ExecutionVertex 可能会有多个 Execution。Execution 始终由ExecutionAttemptID标识。JobManager 和 TaskManager 之间有关任务部署和任务状态更新的所有消息始终使用 ExecutionAttemptID 来寻址消息接收者。
public interface ExecutionGraph extends AccessExecutionGraph {

    void start(@Nonnull ComponentMainThreadExecutor jobMasterMainThreadExecutor);

    SchedulingTopology getSchedulingTopology();

    void enableCheckpointing(
            CheckpointCoordinatorConfiguration chkConfig,
            List<MasterTriggerRestoreHook<?>> masterHooks,
            CheckpointIDCounter checkpointIDCounter,
            CompletedCheckpointStore checkpointStore,
            StateBackend checkpointStateBackend,
            CheckpointStorage checkpointStorage,
            CheckpointStatsTracker statsTracker,
            CheckpointsCleaner checkpointsCleaner,
            String changelogStorage);

    @Nullable
    CheckpointCoordinator getCheckpointCoordinator();

    @Nullable
    CheckpointStatsTracker getCheckpointStatsTracker();

    KvStateLocationRegistry getKvStateLocationRegistry();

    void setJsonPlan(String jsonPlan);

    Configuration getJobConfiguration();

    Throwable getFailureCause();

    @Override
    Iterable<ExecutionJobVertex> getVerticesTopologically();

    @Override
    Iterable<ExecutionVertex> getAllExecutionVertices();

    @Override
    ExecutionJobVertex getJobVertex(JobVertexID id);

    @Override
    Map<JobVertexID, ExecutionJobVertex> getAllVertices();

    /**
     * Gets the number of restarts, including full restarts and fine grained restarts. If a recovery
     * is currently pending, this recovery is included in the count.
     *
     * @return The number of restarts so far
     */
    //获取重启次数，包括完全重启和细粒度重启。如果恢复当前处于待处理状态，则此恢复将包含在计数中。
    //返回：
    //迄今为止重启的次数
    long getNumberOfRestarts();

    Map<IntermediateDataSetID, IntermediateResult> getAllIntermediateResults();

    /**
     * Gets the intermediate result partition by the given partition ID, or throw an exception if
     * the partition is not found.
     *
     * @param id of the intermediate result partition
     * @return intermediate result partition
     */
    //根据给定的分区ID获取中间结果分区，若未找到分区则抛出异常。
    //参数：
    //id – 中间结果分区
    //返回：
    //中间结果划分
    IntermediateResultPartition getResultPartitionOrThrow(final IntermediateResultPartitionID id);

    /**
     * Merges all accumulator results from the tasks previously executed in the Executions.
     *
     * @return The accumulator map
     */
    //合并执行中先前执行的任务的所有累加器结果。
    //返回：
    //累加器图
    Map<String, OptionalFailure<Accumulator<?, ?>>> aggregateUserAccumulators();

    /**
     * Updates the accumulators during the runtime of a job. Final accumulator results are
     * transferred through the UpdateTaskExecutionState message.
     *
     * @param accumulatorSnapshot The serialized flink and user-defined accumulators
     */
    //在作业运行时更新累加器。最终累加器结果通过 UpdateTaskExecutionState 消息传输。
    //参数：
    //accumulatorSnapshot – 序列化的 flink 和用户定义的累加器
    void updateAccumulators(AccumulatorSnapshot accumulatorSnapshot);

    void setInternalTaskFailuresListener(InternalFailuresListener internalTaskFailuresListener);

    void attachJobGraph(
            List<JobVertex> topologicallySorted, JobManagerJobMetricGroup jobManagerJobMetricGroup)
            throws JobException;

    void transitionToRunning();

    void cancel();

    /**
     * Suspends the current ExecutionGraph.
     *
     * <p>The JobStatus will be directly set to {@link JobStatus#SUSPENDED} iff the current state is
     * not a terminal state. All ExecutionJobVertices will be canceled and the onTerminalState() is
     * executed.
     *
     * <p>The {@link JobStatus#SUSPENDED} state is a local terminal state which stops the execution
     * of the job but does not remove the job from the HA job store so that it can be recovered by
     * another JobManager.
     *
     * @param suspensionCause Cause of the suspension
     */
    //暂停当前的 ExecutionGraph。
    //如果当前状态不是终止状态，则 JobStatus 将直接设置为JobStatus. SUSPENDED所有 ExecutionJobVertices 都将被取消，并且 onTerminalState() 将被执行。
    //JobStatus. SUSPENDED状态是一个本地终端状态，它停止作业的执行，但不会从 HA 作业存储中删除该作业，以便其他 JobManager 可以恢复该作业。
    //参数：
    //suspensionCause – 暂停的原因
    void suspend(Throwable suspensionCause);

    void failJob(Throwable cause, long timestamp);

    /**
     * Returns the termination future of this {@link ExecutionGraph}. The termination future is
     * completed with the terminal {@link JobStatus} once the ExecutionGraph reaches this terminal
     * state and all {@link Execution} have been terminated.
     *
     * @return Termination future of this {@link ExecutionGraph}.
     */
    //返回此ExecutionGraph的终止future。一旦 ExecutionGraph 达到此终止状态并且所有Execution都已终止，终止未来将以终端JobStatus完成。
    //返回：
    //此ExecutionGraph的终止future
    CompletableFuture<JobStatus> getTerminationFuture();

    @VisibleForTesting
    JobStatus waitUntilTerminal() throws InterruptedException;

    boolean transitionState(JobStatus current, JobStatus newState);

    void incrementRestarts();

    void initFailureCause(Throwable t, long timestamp);

    /**
     * Updates the state of one of the ExecutionVertex's Execution attempts. If the new status if
     * "FINISHED", this also updates the accumulators.
     *
     * @param state The state update.
     * @return True, if the task update was properly applied, false, if the execution attempt was
     *     not found.
     */
    //更新 ExecutionVertex 的一次执行尝试的状态。如果新状态为“已完成”，则这也会更新累加器。
    //参数：
    //state状态更新。
    //返回：
    //如果任务更新正确应用则为 True，如果未发现执行尝试则为 false。
    boolean updateState(TaskExecutionStateTransition state);

    Map<ExecutionAttemptID, Execution> getRegisteredExecutions();

    void registerJobStatusListener(JobStatusListener listener);

    ResultPartitionAvailabilityChecker getResultPartitionAvailabilityChecker();

    int getNumFinishedVertices();

    @Nonnull
    ComponentMainThreadExecutor getJobMasterMainThreadExecutor();

    default void initializeJobVertex(ExecutionJobVertex ejv, long createTimestamp)
            throws JobException {
        initializeJobVertex(
                ejv,
                createTimestamp,
                VertexInputInfoComputationUtils.computeVertexInputInfos(
                        ejv, getAllIntermediateResults()::get));
    }

    /**
     * Initialize the given execution job vertex, mainly includes creating execution vertices
     * according to the parallelism, and connecting to the predecessors.
     *
     * @param ejv The execution job vertex that needs to be initialized.
     * @param createTimestamp The timestamp for creating execution vertices, used to initialize the
     *     first Execution with.
     * @param jobVertexInputInfos The input infos of this job vertex.
     */
    //初始化给定的执行作业顶点，主要包括根据并行度创建执行顶点，并与其前驱进行连接。
    //参数：
    //ejv – 需要初始化的执行作业顶点。
    //createTimestamp – 创建执行顶点的时间戳，用于初始化第一个执行。
    //jobVertexInputInfos – 此作业顶点的输入信息。
    void initializeJobVertex(
            ExecutionJobVertex ejv,
            long createTimestamp,
            Map<IntermediateDataSetID, JobVertexInputInfo> jobVertexInputInfos)
            throws JobException;

    /**
     * Notify that some job vertices have been newly initialized, execution graph will try to update
     * scheduling topology.
     *
     * @param vertices The execution job vertices that are newly initialized.
     */
    //通知一些作业顶点已被重新初始化，执行图将尝试更新调度拓扑。
    //参数：
    //vertices – 新初始化的执行作业顶点。
    void notifyNewlyInitializedJobVertices(List<ExecutionJobVertex> vertices);

    Optional<String> findVertexWithAttempt(final ExecutionAttemptID attemptId);

    Optional<AccessExecution> findExecution(final ExecutionAttemptID attemptId);
}
