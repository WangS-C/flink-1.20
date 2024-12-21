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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.checkpoint.SnapshotType;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.streaming.api.checkpoint.ExternallyInducedSource;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxDefaultAction;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FatalExitExceptionHandler;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link StreamTask} for executing a {@link StreamSource}.
 *
 * <p>One important aspect of this is that the checkpointing and the emission of elements must never
 * occur at the same time. The execution must be serial. This is achieved by having the contract
 * with the {@link SourceFunction} that it must only modify its state or emit elements in a
 * synchronized block that locks on the lock Object. Also, the modification of the state and the
 * emission of elements must happen in the same block of code that is protected by the synchronized
 * block.
 *
 * @param <OUT> Type of the output elements of this source.
 * @param <SRC> Type of the source function for the stream source operator
 * @param <OP> Type of the stream source operator
 * @deprecated This class is based on the {@link
 *     org.apache.flink.streaming.api.functions.source.SourceFunction} API, which is due to be
 *     removed. Use the new {@link org.apache.flink.api.connector.source.Source} API instead.
 */
//用于执行StreamSource的StreamTask 。
//其中一个重要的方面是检查点设置和元素发射绝不能同时发生。
// 执行必须是串行的。这是通过与SourceFunction签订合同来实现的，即它只能修改其状态或在锁定对象的同步块中发出元素。
// 此外，状态的修改和元素的发射必须发生在受同步块保护的同一代码块中。
@Deprecated
@Internal
public class SourceStreamTask<
                OUT, SRC extends SourceFunction<OUT>, OP extends StreamSource<OUT, SRC>>
        extends StreamTask<OUT, OP> {

    private final LegacySourceFunctionThread sourceThread;
    private final Object lock;

    private volatile boolean externallyInducedCheckpoints;

    private final AtomicBoolean stopped = new AtomicBoolean(false);

    private enum FinishingReason {
        END_OF_DATA(StopMode.DRAIN),
        STOP_WITH_SAVEPOINT_DRAIN(StopMode.DRAIN),
        STOP_WITH_SAVEPOINT_NO_DRAIN(StopMode.NO_DRAIN);

        private final StopMode stopMode;

        FinishingReason(StopMode stopMode) {
            this.stopMode = stopMode;
        }

        StopMode toStopMode() {
            return this.stopMode;
        }
    }

    /**
     * Indicates whether this Task was purposefully finished, in this case we want to ignore
     * exceptions thrown after finishing, to ensure shutdown works smoothly.
     *
     * <p>Moreover we differentiate drain and no drain cases to see if we need to call finish() on
     * the operators.
     */
    //指示此任务是否有意完成，在这种情况下，我们希望忽略完成后引发的异常，以确保关闭顺利进行。
    //此外，我们区分排出和无排出情况，看看是否需要在运算符上调用 finish()。
    private volatile FinishingReason finishingReason = FinishingReason.END_OF_DATA;

    public SourceStreamTask(Environment env) throws Exception {
        this(env, new Object());
    }

    private SourceStreamTask(Environment env, Object lock) throws Exception {
        super(
                env,
                null,
                FatalExitExceptionHandler.INSTANCE,
                StreamTaskActionExecutor.synchronizedExecutor(lock));
        this.lock = Preconditions.checkNotNull(lock);
        this.sourceThread = new LegacySourceFunctionThread();

        getEnvironment().getMetricGroup().getIOMetricGroup().setEnableBusyTime(false);
    }

    //SourceStreamTask子类的init()方法没有太多实质性的操作，
    //主要是判断SourceFunction是不是ExternallyInducedSource类型，
    //是的话为SourceFunction设置checkpoint相关的东西等。
    @Override
    protected void init() {
        // we check if the source is actually inducing the checkpoints, rather
        // than the trigger
        //我们检查源是否实际上是诱导检查点，而不是触发器
        SourceFunction<?> source = mainOperator.getUserFunction();
        if (source instanceof ExternallyInducedSource) {
            externallyInducedCheckpoints = true;

            ExternallyInducedSource.CheckpointTrigger triggerHook =
                    new ExternallyInducedSource.CheckpointTrigger() {

                        @Override
                        public void triggerCheckpoint(long checkpointId) throws FlinkException {
                            // TODO - we need to see how to derive those. We should probably not
                            // encode this in the
                            // TODO -   source's trigger message, but do a handshake in this task
                            // between the trigger
                            // TODO -   message from the master, and the source's trigger
                            // notification
                            //我们需要看看如何得出这些。我们可能不应该源的触发消息中对此进行编码，
                            // 而是在此任务中，在来自主机的触发消息和源的触发通知之间进行握手
                            final CheckpointOptions checkpointOptions =
                                    CheckpointOptions.forConfig(
                                            CheckpointType.CHECKPOINT,
                                            CheckpointStorageLocationReference.getDefault(),
                                            configuration.isExactlyOnceCheckpointMode(),
                                            configuration.isUnalignedCheckpointsEnabled(),
                                            configuration.getAlignedCheckpointTimeout().toMillis());
                            final long timestamp = System.currentTimeMillis();

                            final CheckpointMetaData checkpointMetaData =
                                    new CheckpointMetaData(checkpointId, timestamp, timestamp);

                            try {
                                SourceStreamTask.super
                                        .triggerCheckpointAsync(
                                                checkpointMetaData, checkpointOptions)
                                        .get();
                            } catch (RuntimeException e) {
                                throw e;
                            } catch (Exception e) {
                                throw new FlinkException(e.getMessage(), e);
                            }
                        }
                    };

            ((ExternallyInducedSource<?, ?>) source).setCheckpointTrigger(triggerHook);
        }
        getEnvironment()
                .getMetricGroup()
                .getIOMetricGroup()
                .gauge(
                        MetricNames.CHECKPOINT_START_DELAY_TIME,
                        this::getAsyncCheckpointStartDelayNanos);
        recordWriter.setMaxOverdraftBuffersPerGate(0);
    }

    @Override
    protected void advanceToEndOfEventTime() throws Exception {
        operatorChain.getMainOperatorOutput().emitWatermark(Watermark.MAX_WATERMARK);
    }

    @Override
    protected void cleanUpInternal() {
        // does not hold any resources, so no cleanup needed
        // 不持有任何资源，因此无需清理
    }

    @Override
    protected void processInput(MailboxDefaultAction.Controller controller) throws Exception {

        controller.suspendDefaultAction();

        // Against the usual contract of this method, this implementation is not step-wise but
        // blocking instead for
        // compatibility reasons with the current source interface (source functions run as a loop,
        // not in steps).
        //与此方法的通常约定不同，此实现不是逐步实现，
        // 而是出于与当前源接口的兼容性原因而阻塞（源函数作为循环运行，而不是逐步运行）。
        sourceThread.setTaskDescription(getName());

        sourceThread.start();

        sourceThread
                .getCompletionFuture()
                .whenComplete(
                        (Void ignore, Throwable sourceThreadThrowable) -> {
                            if (sourceThreadThrowable != null) {
                                mailboxProcessor.reportThrowable(sourceThreadThrowable);
                            } else {
                                notifyEndOfData();
                                mailboxProcessor.suspend();
                            }
                        });
    }

    @Override
    protected void cancelTask() {
        if (stopped.compareAndSet(false, true)) {
            cancelOperator();
        }
    }

    private void cancelOperator() {
        try {
            if (mainOperator != null) {
                mainOperator.cancel();
            }
        } finally {
            if (sourceThread.isAlive()) {
                interruptSourceThread();
            } else if (!sourceThread.getCompletionFuture().isDone()) {
                // sourceThread not alive and completion future not done means source thread
                // didn't start and we need to manually complete the future
                //sourceThread 不存在并且完成 future 未完成意味着源线程没有启动，我们需要手动完成 future
                sourceThread.getCompletionFuture().complete(null);
            }
        }
    }

    @Override
    public void maybeInterruptOnCancel(
            Thread toInterrupt, @Nullable String taskName, @Nullable Long timeout) {
        super.maybeInterruptOnCancel(toInterrupt, taskName, timeout);
        interruptSourceThread();
    }

    private void interruptSourceThread() {
        // Nothing need to do if the source is finished on restore
        //如果源已完成恢复，则无需执行任何操作
        if (operatorChain != null && operatorChain.isTaskDeployedAsFinished()) {
            return;
        }

        if (sourceThread.isAlive()) {
            sourceThread.interrupt();
        }
    }

    @Override
    protected CompletableFuture<Void> getCompletionFuture() {
        return sourceThread.getCompletionFuture();
    }

    // ------------------------------------------------------------------------
    //  Checkpointing
    // ------------------------------------------------------------------------

    @Override
    public CompletableFuture<Boolean> triggerCheckpointAsync(
            CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions) {
        if (!externallyInducedCheckpoints) {
            if (isSynchronousSavepoint(checkpointOptions.getCheckpointType())) {
                return triggerStopWithSavepointAsync(checkpointMetaData, checkpointOptions);
            } else {
                return super.triggerCheckpointAsync(checkpointMetaData, checkpointOptions);
            }
        } else if (checkpointOptions.getCheckpointType().equals(CheckpointType.FULL_CHECKPOINT)) {
            // see FLINK-25256
            throw new IllegalStateException(
                    "Using externally induced sources, we can not enforce taking a full checkpoint."
                            + "If you are restoring from a snapshot in NO_CLAIM mode, please use"
                            + " CLAIM mode.");
        } else {
            // we do not trigger checkpoints here, we simply state whether we can trigger them
            // 我们这里不触发检查点，我们只是说明是否可以触发它们
            synchronized (lock) {
                return CompletableFuture.completedFuture(isRunning());
            }
        }
    }

    private boolean isSynchronousSavepoint(SnapshotType snapshotType) {
        return snapshotType.isSavepoint() && ((SavepointType) snapshotType).isSynchronous();
    }

    private CompletableFuture<Boolean> triggerStopWithSavepointAsync(
            CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions) {
        mainMailboxExecutor.execute(
                () ->
                        stopOperatorForStopWithSavepoint(
                                checkpointMetaData.getCheckpointId(),
                                ((SavepointType) checkpointOptions.getCheckpointType())
                                        .shouldDrain()),
                "stop legacy source for stop-with-savepoint --drain");
        return sourceThread
                .getCompletionFuture()
                .thenCompose(
                        ignore ->
                                super.triggerCheckpointAsync(
                                        checkpointMetaData, checkpointOptions));
    }

    private void stopOperatorForStopWithSavepoint(long checkpointId, boolean drain) {
        setSynchronousSavepoint(checkpointId);
        finishingReason =
                drain
                        ? FinishingReason.STOP_WITH_SAVEPOINT_DRAIN
                        : FinishingReason.STOP_WITH_SAVEPOINT_NO_DRAIN;
        if (mainOperator != null) {
            mainOperator.stop();
        }
    }

    @Override
    protected void declineCheckpoint(long checkpointId) {
        if (!externallyInducedCheckpoints) {
            super.declineCheckpoint(checkpointId);
        }
    }

    /** Runnable that executes the source function in the head operator. */
    //执行头操作符中的源函数的Runnable。
    private class LegacySourceFunctionThread extends Thread {

        private final CompletableFuture<Void> completionFuture;

        LegacySourceFunctionThread() {
            this.completionFuture = new CompletableFuture<>();
        }

        @Override
        public void run() {
            try {
                if (!operatorChain.isTaskDeployedAsFinished()) {
                    LOG.debug(
                            "Legacy source {} skip execution since the task is finished on restore",
                            getTaskNameWithSubtaskAndId());
                    mainOperator.run(lock, operatorChain);
                }
                completeProcessing();
                completionFuture.complete(null);
            } catch (Throwable t) {
                // Note, t can be also an InterruptedException
                // 注意，t也可以是InterruptedException
                if (isCanceled()
                        && ExceptionUtils.findThrowable(t, InterruptedException.class)
                                .isPresent()) {
                    completionFuture.completeExceptionally(new CancelTaskException(t));
                } else {
                    completionFuture.completeExceptionally(t);
                }
            }
        }

        private void completeProcessing() throws InterruptedException, ExecutionException {
            if (!isCanceled() && !isFailing()) {
                mainMailboxExecutor
                        .submit(
                                () -> {
                                    // theoretically the StreamSource can implement BoundedOneInput,
                                    // so we need to call it here
                                    //理论上StreamSource可以实现BoundedOneInput，所以我们需要在这里调用它
                                    final StopMode stopMode = finishingReason.toStopMode();
                                    if (stopMode == StopMode.DRAIN) {
                                        operatorChain.endInput(1);
                                    }
                                    endData(stopMode);
                                },
                                "SourceStreamTask finished processing data.")
                        .get();
            }
        }

        public void setTaskDescription(final String taskDescription) {
            setName("Legacy Source Thread - " + taskDescription);
        }

        /**
         * @return future that is completed once this thread completes. If this task {@link
         *     #isFailing()} and this thread is not alive (e.g. not started) returns a normally
         *     completed future.
         */
        //返回：
        //一旦该线程完成，未来即完成。如果此任务isFailing()并且此线程不活动（例如未启动），则返回正常完成的 future。
        CompletableFuture<Void> getCompletionFuture() {
            return isFailing() && !isAlive()
                    ? CompletableFuture.completedFuture(null)
                    : completionFuture;
        }
    }
}
