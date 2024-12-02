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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorInfo;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.concurrent.FutureUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

/**
 * All the logic related to taking checkpoints of the {@link OperatorCoordinator}s.
 *
 * <p>NOTE: This class has a simplified error handling logic. If one of the several coordinator
 * checkpoints fail, no cleanup is triggered for the other concurrent ones. That is okay, since they
 * all produce just byte[] as the result. We have to change that once we allow then to create
 * external resources that actually need to be cleaned up.
 */
//与获取OperatorCoordinator的检查点相关的所有逻辑。
//注意: 这个类有一个简化的错误处理逻辑。如果多个协调器检查点之一失败，则不会为其他并发检查点触发清除。
//这是好的，因为他们都只产生字节 [] 作为结果。我们必须改变，一旦我们允许，然后创建实际需要清理的外部资源。
final class OperatorCoordinatorCheckpoints {

    public static CompletableFuture<CoordinatorSnapshot> triggerCoordinatorCheckpoint(
            final OperatorCoordinatorCheckpointContext coordinatorContext, final long checkpointId)
            throws Exception {

        final CompletableFuture<byte[]> checkpointFuture = new CompletableFuture<>();
        coordinatorContext.checkpointCoordinator(checkpointId, checkpointFuture);

        return checkpointFuture.thenApply(
                (state) ->
                        new CoordinatorSnapshot(
                                coordinatorContext,
                                new ByteStreamStateHandle(
                                        coordinatorContext.operatorId().toString(), state)));
    }

    public static CompletableFuture<AllCoordinatorSnapshots> triggerAllCoordinatorCheckpoints(
            final Collection<OperatorCoordinatorCheckpointContext> coordinators,
            final long checkpointId)
            throws Exception {

        final Collection<CompletableFuture<CoordinatorSnapshot>> individualSnapshots =
                new ArrayList<>(coordinators.size());

        for (final OperatorCoordinatorCheckpointContext coordinator : coordinators) {
            final CompletableFuture<CoordinatorSnapshot> checkpointFuture =
                    //触发协调器检查点
                    triggerCoordinatorCheckpoint(coordinator, checkpointId);
            individualSnapshots.add(checkpointFuture);
        }

        return FutureUtils.combineAll(individualSnapshots).thenApply(AllCoordinatorSnapshots::new);
    }

    public static CompletableFuture<Void> triggerAndAcknowledgeAllCoordinatorCheckpoints(
            final Collection<OperatorCoordinatorCheckpointContext> coordinators,
            final PendingCheckpoint checkpoint,
            final Executor acknowledgeExecutor)
            throws Exception {

        final CompletableFuture<AllCoordinatorSnapshots> snapshots =
                //触发所有协调器检查点
                triggerAllCoordinatorCheckpoints(coordinators, checkpoint.getCheckpointID());

        return snapshots.thenAcceptAsync(
                (allSnapshots) -> {
                    try {
                        //确认所有协调员
                        acknowledgeAllCoordinators(checkpoint, allSnapshots.snapshots);
                    } catch (Exception e) {
                        throw new CompletionException(e);
                    }
                },
                acknowledgeExecutor);
    }

    public static CompletableFuture<Void>
            triggerAndAcknowledgeAllCoordinatorCheckpointsWithCompletion(
                    final Collection<OperatorCoordinatorCheckpointContext> coordinators,
                    final PendingCheckpoint checkpoint,
                    final Executor acknowledgeExecutor)
                    throws CompletionException {

        try {
            //触发并确认所有协调器检查点
            return triggerAndAcknowledgeAllCoordinatorCheckpoints(
                    coordinators, checkpoint, acknowledgeExecutor);
        } catch (Exception e) {
            throw new CompletionException(e);
        }
    }

    // ------------------------------------------------------------------------

    private static void acknowledgeAllCoordinators(
            PendingCheckpoint checkpoint, Collection<CoordinatorSnapshot> snapshots)
            throws CheckpointException {
        for (final CoordinatorSnapshot snapshot : snapshots) {
            final PendingCheckpoint.TaskAcknowledgeResult result =
                    //确认协调员状态
                    checkpoint.acknowledgeCoordinatorState(snapshot.coordinator, snapshot.state);

            if (result != PendingCheckpoint.TaskAcknowledgeResult.SUCCESS) {
                final String errorMessage =
                        "Coordinator state not acknowledged successfully: " + result;
                final Throwable error =
                        checkpoint.isDisposed() ? checkpoint.getFailureCause() : null;

                CheckpointFailureReason reason = CheckpointFailureReason.TRIGGER_CHECKPOINT_FAILURE;
                if (error != null) {
                    final Optional<IOException> ioExceptionOptional =
                            ExceptionUtils.findThrowable(error, IOException.class);
                    if (ioExceptionOptional.isPresent()) {
                        reason = CheckpointFailureReason.IO_EXCEPTION;
                    }

                    throw new CheckpointException(errorMessage, reason, error);
                } else {
                    throw new CheckpointException(errorMessage, reason);
                }
            }
        }
    }

    // ------------------------------------------------------------------------

    static final class AllCoordinatorSnapshots {

        private final Collection<CoordinatorSnapshot> snapshots;

        AllCoordinatorSnapshots(Collection<CoordinatorSnapshot> snapshots) {
            this.snapshots = snapshots;
        }

        public Iterable<CoordinatorSnapshot> snapshots() {
            return snapshots;
        }
    }

    static final class CoordinatorSnapshot {

        final OperatorInfo coordinator;
        final ByteStreamStateHandle state;

        CoordinatorSnapshot(OperatorInfo coordinator, ByteStreamStateHandle state) {
            this.coordinator = coordinator;
            this.state = state;
        }
    }
}
