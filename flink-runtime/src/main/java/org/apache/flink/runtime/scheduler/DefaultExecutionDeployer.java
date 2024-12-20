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
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** Default implementation of {@link ExecutionDeployer}. */
public class DefaultExecutionDeployer implements ExecutionDeployer {

    private final Logger log;

    private final ExecutionSlotAllocator executionSlotAllocator;

    private final ExecutionOperations executionOperations;

    private final ExecutionVertexVersioner executionVertexVersioner;

    private final Time partitionRegistrationTimeout;

    private final BiConsumer<ExecutionVertexID, AllocationID> allocationReservationFunc;

    private final ComponentMainThreadExecutor mainThreadExecutor;

    private DefaultExecutionDeployer(
            final Logger log,
            final ExecutionSlotAllocator executionSlotAllocator,
            final ExecutionOperations executionOperations,
            final ExecutionVertexVersioner executionVertexVersioner,
            final Time partitionRegistrationTimeout,
            final BiConsumer<ExecutionVertexID, AllocationID> allocationReservationFunc,
            final ComponentMainThreadExecutor mainThreadExecutor) {

        this.log = checkNotNull(log);
        this.executionSlotAllocator = checkNotNull(executionSlotAllocator);
        this.executionOperations = checkNotNull(executionOperations);
        this.executionVertexVersioner = checkNotNull(executionVertexVersioner);
        this.partitionRegistrationTimeout = checkNotNull(partitionRegistrationTimeout);
        this.allocationReservationFunc = checkNotNull(allocationReservationFunc);
        this.mainThreadExecutor = checkNotNull(mainThreadExecutor);
    }

    @Override
    public void allocateSlotsAndDeploy(
            final List<Execution> executionsToDeploy,
            final Map<ExecutionVertexID, ExecutionVertexVersion> requiredVersionByVertex) {
        //验证执行状态
        validateExecutionStates(executionsToDeploy);

        //切换至Scheduled
        transitionToScheduled(executionsToDeploy);

        final Map<ExecutionAttemptID, ExecutionSlotAssignment> executionSlotAssignmentMap =
                //分配Slot
                allocateSlotsFor(executionsToDeploy);

        final List<ExecutionDeploymentHandle> deploymentHandles =
                //创建部署句柄
                createDeploymentHandles(
                        executionsToDeploy, requiredVersionByVertex, executionSlotAssignmentMap);

        //等待所有插槽并部署
        waitForAllSlotsAndDeploy(deploymentHandles);
    }

    private void validateExecutionStates(final Collection<Execution> executionsToDeploy) {
        executionsToDeploy.forEach(
                e ->
                        checkState(
                                e.getState() == ExecutionState.CREATED,
                                "Expected execution %s to be in CREATED state, was: %s",
                                e.getAttemptId(),
                                e.getState()));
    }

    private void transitionToScheduled(final List<Execution> executionsToDeploy) {
        executionsToDeploy.forEach(e -> e.transitionState(ExecutionState.SCHEDULED));
    }

    private Map<ExecutionAttemptID, ExecutionSlotAssignment> allocateSlotsFor(
            final List<Execution> executionsToDeploy) {
        final List<ExecutionAttemptID> executionAttemptIds =
                executionsToDeploy.stream()
                        .map(Execution::getAttemptId)
                        .collect(Collectors.toList());
        //分配slot
        return executionSlotAllocator.allocateSlotsFor(executionAttemptIds);
    }

    private List<ExecutionDeploymentHandle> createDeploymentHandles(
            final List<Execution> executionsToDeploy,
            final Map<ExecutionVertexID, ExecutionVertexVersion> requiredVersionByVertex,
            final Map<ExecutionAttemptID, ExecutionSlotAssignment> executionSlotAssignmentMap) {
        checkState(executionsToDeploy.size() == executionSlotAssignmentMap.size());
        final List<ExecutionDeploymentHandle> deploymentHandles =
                new ArrayList<>(executionsToDeploy.size());
        for (final Execution execution : executionsToDeploy) {
            final ExecutionSlotAssignment assignment =
                    checkNotNull(executionSlotAssignmentMap.get(execution.getAttemptId()));

            final ExecutionVertexID executionVertexId = execution.getVertex().getID();
            final ExecutionDeploymentHandle deploymentHandle =
                    new ExecutionDeploymentHandle(
                            execution, assignment, requiredVersionByVertex.get(executionVertexId));
            deploymentHandles.add(deploymentHandle);
        }

        return deploymentHandles;
    }

    private void waitForAllSlotsAndDeploy(final List<ExecutionDeploymentHandle> deploymentHandles) {
        FutureUtils.assertNoException(
                //分配所有资源并注册生成的分区
                assignAllResourcesAndRegisterProducedPartitions(deploymentHandles)
                        //部署全部
                        .handle(deployAll(deploymentHandles)));
    }

    //为每一个Execution分配资源和注册生产分区。
    private CompletableFuture<Void> assignAllResourcesAndRegisterProducedPartitions(
            final List<ExecutionDeploymentHandle> deploymentHandles) {
        final List<CompletableFuture<Void>> resultFutures = new ArrayList<>();
        for (ExecutionDeploymentHandle deploymentHandle : deploymentHandles) {
            final CompletableFuture<Void> resultFuture =
                    deploymentHandle
                            .getLogicalSlotFuture()
                            //为Execution分配slot资源
                            .handle(assignResource(deploymentHandle))
                            //根据Execution的下游消费节点数量，为Execution(Task)设置下游所有消费分区链接信息。
                            .thenCompose(registerProducedPartitions(deploymentHandle))
                            .handle(
                                    (ignore, throwable) -> {
                                        if (throwable != null) {
                                            handleTaskDeploymentFailure(
                                                    deploymentHandle.getExecution(), throwable);
                                        }
                                        return null;
                                    });

            resultFutures.add(resultFuture);
        }
        return FutureUtils.waitForAll(resultFutures);
    }

    private BiFunction<Void, Throwable, Void> deployAll(
            final List<ExecutionDeploymentHandle> deploymentHandles) {
        return (ignored, throwable) -> {
            propagateIfNonNull(throwable);
            for (final ExecutionDeploymentHandle deploymentHandle : deploymentHandles) {
                final CompletableFuture<LogicalSlot> slotAssigned =
                        deploymentHandle.getLogicalSlotFuture();
                checkState(slotAssigned.isDone());

                FutureUtils.assertNoException(
                        //部署或处理错误
                        slotAssigned.handle(deployOrHandleError(deploymentHandle)));
            }
            return null;
        };
    }

    private static void propagateIfNonNull(final Throwable throwable) {
        if (throwable != null) {
            throw new CompletionException(throwable);
        }
    }

    //负责为Execution分配slot资源。
    private BiFunction<LogicalSlot, Throwable, LogicalSlot> assignResource(
            final ExecutionDeploymentHandle deploymentHandle) {

        return (logicalSlot, throwable) -> {
            final ExecutionVertexVersion requiredVertexVersion =
                    deploymentHandle.getRequiredVertexVersion();
            final Execution execution = deploymentHandle.getExecution();

            if (execution.getState() != ExecutionState.SCHEDULED
                    || executionVertexVersioner.isModified(requiredVertexVersion)) {
                if (throwable == null) {
                    log.debug(
                            "Refusing to assign slot to execution {} because this deployment was "
                                    + "superseded by another deployment",
                            deploymentHandle.getExecutionAttemptId());
                    releaseSlotIfPresent(logicalSlot);
                }
                return null;
            }

            // throw exception only if the execution version is not outdated.
            // this ensures that canceling a pending slot request does not fail
            // a task which is about to cancel.
            //仅当执行版本未过时时才引发异常。这确保取消挂起的时隙请求不会使将要取消的任务失败。
            if (throwable != null) {
                throw new CompletionException(maybeWrapWithNoResourceAvailableException(throwable));
            }

            //设置assignedResource成员变量的值，代表Execution被分配slot资源，
            //最后在ExecutionVertex中记录最新一次Execution运行实例的资源分配地址信息
            if (!execution.tryAssignResource(logicalSlot)) {
                throw new IllegalStateException(
                        "Could not assign resource "
                                + logicalSlot
                                + " to execution "
                                + execution
                                + '.');
            }

            // We only reserve the latest execution of an execution vertex. Because it may cause
            // problems to reserve multiple slots for one execution vertex. Besides that, slot
            // reservation is for local recovery and therefore is only needed by streaming jobs, in
            // which case an execution vertex will have one only current execution.
            //我们只保留执行顶点的最新执行。
            //因为它可能会导致为一个执行顶点保留多个插槽的问题。
            //除此之外，槽预留用于本地恢复，因此仅流作业需要，在这种情况下，执行顶点将只有一个当前执行。
            allocationReservationFunc.accept(
                    execution.getAttemptId().getExecutionVertexId(), logicalSlot.getAllocationId());

            return logicalSlot;
        };
    }

    private static void releaseSlotIfPresent(@Nullable final LogicalSlot logicalSlot) {
        if (logicalSlot != null) {
            logicalSlot.releaseSlot(null);
        }
    }

    private static Throwable maybeWrapWithNoResourceAvailableException(final Throwable failure) {
        final Throwable strippedThrowable = ExceptionUtils.stripCompletionException(failure);
        if (strippedThrowable instanceof TimeoutException) {
            return new NoResourceAvailableException(
                    "Could not allocate the required slot within slot request timeout. "
                            + "Please make sure that the cluster has enough resources.",
                    failure);
        } else {
            return failure;
        }
    }

    //根据Execution的下游消费节点数量，为Execution(Task)设置下游所有消费分区链接信息。
    private Function<LogicalSlot, CompletableFuture<Void>> registerProducedPartitions(
            final ExecutionDeploymentHandle deploymentHandle) {

        return logicalSlot -> {
            // a null logicalSlot means the slot assignment is skipped, in which case
            // the produced partition registration process can be skipped as well
            //空的逻辑槽意味着槽分配被跳过，在这种情况下，生成的分区注册过程也可以被跳过
            if (logicalSlot != null) {
                final Execution execution = deploymentHandle.getExecution();
                final CompletableFuture<Void> partitionRegistrationFuture =
                        //注册生生产者分区
                        execution.registerProducedPartitions(logicalSlot.getTaskManagerLocation());

                return FutureUtils.orTimeout(
                        partitionRegistrationFuture,
                        partitionRegistrationTimeout.toMilliseconds(),
                        TimeUnit.MILLISECONDS,
                        mainThreadExecutor,
                        String.format(
                                "Registering produced partitions for execution %s timed out after %d ms.",
                                execution.getAttemptId(),
                                partitionRegistrationTimeout.toMilliseconds()));
            } else {
                return FutureUtils.completedVoidFuture();
            }
        };
    }

    private BiFunction<Object, Throwable, Void> deployOrHandleError(
            final ExecutionDeploymentHandle deploymentHandle) {

        return (ignored, throwable) -> {
            final ExecutionVertexVersion requiredVertexVersion =
                    deploymentHandle.getRequiredVertexVersion();
            final Execution execution = deploymentHandle.getExecution();

            if (execution.getState() != ExecutionState.SCHEDULED
                    || executionVertexVersioner.isModified(requiredVertexVersion)) {
                if (throwable == null) {
                    log.debug(
                            "Refusing to assign slot to execution {} because this deployment was "
                                    + "superseded by another deployment",
                            deploymentHandle.getExecutionAttemptId());
                }
                return null;
            }

            if (throwable == null) {
                //安全部署任务
                deployTaskSafe(execution);
            } else {
                handleTaskDeploymentFailure(execution, throwable);
            }
            return null;
        };
    }

    private void deployTaskSafe(final Execution execution) {
        try {
            executionOperations.deploy(execution);
        } catch (Throwable e) {
            handleTaskDeploymentFailure(execution, e);
        }
    }

    private void handleTaskDeploymentFailure(final Execution execution, final Throwable error) {
        executionOperations.markFailed(execution, error);
    }

    private static class ExecutionDeploymentHandle {

        private final Execution execution;

        private final ExecutionSlotAssignment executionSlotAssignment;

        private final ExecutionVertexVersion requiredVertexVersion;

        ExecutionDeploymentHandle(
                final Execution execution,
                final ExecutionSlotAssignment executionSlotAssignment,
                final ExecutionVertexVersion requiredVertexVersion) {
            this.execution = checkNotNull(execution);
            this.executionSlotAssignment = checkNotNull(executionSlotAssignment);
            this.requiredVertexVersion = checkNotNull(requiredVertexVersion);
        }

        Execution getExecution() {
            return execution;
        }

        ExecutionAttemptID getExecutionAttemptId() {
            return execution.getAttemptId();
        }

        CompletableFuture<LogicalSlot> getLogicalSlotFuture() {
            return executionSlotAssignment.getLogicalSlotFuture();
        }

        ExecutionVertexVersion getRequiredVertexVersion() {
            return requiredVertexVersion;
        }
    }

    /** Factory to instantiate the {@link DefaultExecutionDeployer}. */
    public static class Factory implements ExecutionDeployer.Factory {

        @Override
        public DefaultExecutionDeployer createInstance(
                Logger log,
                ExecutionSlotAllocator executionSlotAllocator,
                ExecutionOperations executionOperations,
                ExecutionVertexVersioner executionVertexVersioner,
                Time partitionRegistrationTimeout,
                BiConsumer<ExecutionVertexID, AllocationID> allocationReservationFunc,
                ComponentMainThreadExecutor mainThreadExecutor) {
            return new DefaultExecutionDeployer(
                    log,
                    executionSlotAllocator,
                    executionOperations,
                    executionVertexVersioner,
                    partitionRegistrationTimeout,
                    allocationReservationFunc,
                    mainThreadExecutor);
        }
    }
}
