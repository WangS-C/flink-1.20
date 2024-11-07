/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionStateUpdateListener;
import org.apache.flink.runtime.executiongraph.MarkPartitionFinishedStrategy;
import org.apache.flink.runtime.executiongraph.VertexAttemptNumberStore;
import org.apache.flink.runtime.jobgraph.JobGraph;

import org.slf4j.Logger;

/** Factory for creating an {@link ExecutionGraph}. */
public interface ExecutionGraphFactory {

    /**
     * Create and restore {@link ExecutionGraph} from the given {@link JobGraph} and services.
     *
     * @param jobGraph jobGraph to initialize the ExecutionGraph with
     * @param completedCheckpointStore completedCheckpointStore to pass to the CheckpointCoordinator
     * @param checkpointsCleaner checkpointsCleaner to pass to the CheckpointCoordinator
     * @param checkpointIdCounter checkpointIdCounter to pass to the CheckpointCoordinator
     * @param partitionLocationConstraint partitionLocationConstraint for this job
     * @param initializationTimestamp initializationTimestamp when the ExecutionGraph was created
     * @param vertexAttemptNumberStore vertexAttemptNumberStore keeping information about the vertex
     *     attempts of previous runs
     * @param vertexParallelismStore vertexMaxParallelismStore keeping information about the vertex
     *     max parallelism settings
     * @param executionStateUpdateListener listener for state transitions of the individual
     *     executions
     * @param log log to use for logging
     * @return restored {@link ExecutionGraph}
     * @throws Exception if the {@link ExecutionGraph} could not be created and restored
     */
    //从给定的JobGraph和服务创建和恢复ExecutionGraph 。
    //参数：
    //jobGraph – 用于初始化 ExecutionGraph 的 jobGraph
    //completedCheckpointStore – CompletedCheckpointStore 传递给 CheckpointCoordinator
    //checkpointsCleaner – checkpointsCleaner 传递给 CheckpointCoordinator
    //checkpointIdCounter – 传递给 CheckpointCoordinator 的 checkpointIdCounter
    //partitionLocationConstraint – 此作业的partitionLocationConstraint
    //initializationTimestamp – 创建 ExecutionGraph 时的初始化时间戳
    //vertexAttemptNumberStore – vertexAttemptNumberStore 保存有关先前运行的顶点尝试的信息
    //vertexParallelismStore – vertexMaxParallelismStore 保存有关顶点最大并行度设置的信息
    //executionStateUpdateListener – 各个执行的状态转换的侦听器 log – 用于记录日志的日志
    //返回：
    //恢复ExecutionGraph
    ExecutionGraph createAndRestoreExecutionGraph(
            JobGraph jobGraph,
            CompletedCheckpointStore completedCheckpointStore,
            CheckpointsCleaner checkpointsCleaner,
            CheckpointIDCounter checkpointIdCounter,
            TaskDeploymentDescriptorFactory.PartitionLocationConstraint partitionLocationConstraint,
            long initializationTimestamp,
            VertexAttemptNumberStore vertexAttemptNumberStore,
            VertexParallelismStore vertexParallelismStore,
            ExecutionStateUpdateListener executionStateUpdateListener,
            MarkPartitionFinishedStrategy markPartitionFinishedStrategy,
            Logger log)
            throws Exception;
}
