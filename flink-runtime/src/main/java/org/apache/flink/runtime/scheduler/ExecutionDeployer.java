/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import org.slf4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/** This deployer is responsible for deploying executions. */
public interface ExecutionDeployer {

    /**
     * Allocate slots and deploy executions.
     *
     * @param executionsToDeploy executions to deploy
     * @param requiredVersionByVertex required versions of the execution vertices. If the actual
     *     version does not match, the deployment of the execution will be rejected.
     */
    //分配槽并部署执行。
    //参数：
    //executionsToDeploy – 要部署的执行
    //requiredVersionByVertex – 执行顶点所需的版本。如果实际版本不匹配，部署执行将被拒绝。
    void allocateSlotsAndDeploy(
            final List<Execution> executionsToDeploy,
            final Map<ExecutionVertexID, ExecutionVertexVersion> requiredVersionByVertex);

    /** Factory to instantiate the {@link ExecutionDeployer}. */
    interface Factory {

        /**
         * Instantiate an {@link ExecutionDeployer} with the given params. Note that the version of
         * an execution vertex will be recorded before scheduling executions for it. The version may
         * change if a global failure happens, or if the job is canceled, or if the execution vertex
         * is restarted when all its current execution are FAILED/CANCELED. Once the version is
         * changed, the previously triggered execution deployment will be skipped.
         *
         * @param log the logger
         * @param executionSlotAllocator the allocator to allocate slots
         * @param executionOperations the operations of executions
         * @param executionVertexVersioner the versioner which records the versions of execution
         *     vertices.
         * @param partitionRegistrationTimeout timeout of partition registration
         * @param allocationReservationFunc function to reserve allocations for local recovery
         * @param mainThreadExecutor the main thread executor
         * @return an instantiated {@link ExecutionDeployer}
         */
        //使用给定参数实例化ExecutionDeployer 。
        //请注意，执行顶点的版本将在调度执行之前被记录。
        //如果发生全局故障，或者作业被取消，或者当当前所有执行都失败/ 取消时重新启动执行顶点，则版本可能会更改。
        //一旦版本变更，之前触发的执行部署将被跳过。
        //参数：
        //log ——记录器
        //executionSlotAllocator – 分配槽的分配器
        //executionOperations – 执行的操作
        //executionVertexVersioner – 记录执行顶点版本的版本控制器。
        //partitionRegistrationTimeout – 分区注册超时
        //allocationReservationFunc – 为本地恢复保留分配的函数
        //mainThreadExecutor – 主线程执行器
        ExecutionDeployer createInstance(
                final Logger log,
                final ExecutionSlotAllocator executionSlotAllocator,
                final ExecutionOperations executionOperations,
                final ExecutionVertexVersioner executionVertexVersioner,
                final Time partitionRegistrationTimeout,
                final BiConsumer<ExecutionVertexID, AllocationID> allocationReservationFunc,
                final ComponentMainThreadExecutor mainThreadExecutor);
    }
}
