/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService.ProcessingTimeCallback;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.execution.RestoreMode;
import org.apache.flink.core.fs.AutoCloseableRegistry;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.security.FlinkSecurityManager;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetricsBuilder;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.InitializationStatus;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.checkpoint.SnapshotType;
import org.apache.flink.runtime.checkpoint.SubTaskInitializationMetricsBuilder;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.checkpoint.channel.SequentialChannelStateReader;
import org.apache.flink.runtime.checkpoint.filemerging.FileMergingSnapshotManager;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.AvailabilityProvider;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.io.network.api.writer.MultipleRecordWriters;
import org.apache.flink.runtime.io.network.api.writer.NonRecordWriter;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.api.writer.RecordWriterBuilder;
import org.apache.flink.runtime.io.network.api.writer.RecordWriterDelegate;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.api.writer.SingleRecordWriter;
import org.apache.flink.runtime.io.network.partition.ChannelStateHolder;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointableTask;
import org.apache.flink.runtime.jobgraph.tasks.CoordinatedTask;
import org.apache.flink.runtime.jobgraph.tasks.TaskInvokable;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStorageAccess;
import org.apache.flink.runtime.state.CheckpointStorageLoader;
import org.apache.flink.runtime.state.CheckpointStorageWorkerView;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.runtime.state.filesystem.FsMergingCheckpointStorageAccess;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.taskmanager.AsyncExceptionHandler;
import org.apache.flink.runtime.taskmanager.AsynchronousException;
import org.apache.flink.runtime.taskmanager.DispatcherThreadFactory;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.graph.NonChainedOutput;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManager;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManagerImpl;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializerImpl;
import org.apache.flink.streaming.runtime.io.DataInputStatus;
import org.apache.flink.streaming.runtime.io.RecordWriterOutput;
import org.apache.flink.streaming.runtime.io.StreamInputProcessor;
import org.apache.flink.streaming.runtime.io.checkpointing.BarrierAlignmentUtil;
import org.apache.flink.streaming.runtime.io.checkpointing.CheckpointBarrierHandler;
import org.apache.flink.streaming.runtime.partitioner.ConfigurableStreamPartitioner;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.mailbox.GaugePeriodTimer;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxDefaultAction;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxDefaultAction.Suspension;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxExecutorFactory;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxMetricsController;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxProcessor;
import org.apache.flink.streaming.runtime.tasks.mailbox.PeriodTimer;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailboxImpl;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FatalExitExceptionHandler;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.MdcUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.clock.SystemClock;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.RunnableWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.configuration.TaskManagerOptions.BUFFER_DEBLOAT_PERIOD;
import static org.apache.flink.runtime.metrics.MetricNames.GATE_RESTORE_DURATION;
import static org.apache.flink.runtime.metrics.MetricNames.INITIALIZE_STATE_DURATION;
import static org.apache.flink.runtime.metrics.MetricNames.MAILBOX_START_DURATION;
import static org.apache.flink.runtime.metrics.MetricNames.READ_OUTPUT_DATA_DURATION;
import static org.apache.flink.streaming.runtime.tasks.SubtaskCheckpointCoordinatorImpl.openChannelStateWriter;
import static org.apache.flink.util.ExceptionUtils.firstOrSuppressed;
import static org.apache.flink.util.Preconditions.checkState;
import static org.apache.flink.util.concurrent.FutureUtils.assertNoException;

/**
 * Base class for all streaming tasks. A task is the unit of local processing that is deployed and
 * executed by the TaskManagers. Each task runs one or more {@link StreamOperator}s which form the
 * Task's operator chain. Operators that are chained together execute synchronously in the same
 * thread and hence on the same stream partition. A common case for these chains are successive
 * map/flatmap/filter tasks.
 *
 * <p>The task chain contains one "head" operator and multiple chained operators. The StreamTask is
 * specialized for the type of the head operator: one-input and two-input tasks, as well as for
 * sources, iteration heads and iteration tails.
 *
 * <p>The Task class deals with the setup of the streams read by the head operator, and the streams
 * produced by the operators at the ends of the operator chain. Note that the chain may fork and
 * thus have multiple ends.
 *
 * <p>The life cycle of the task is set up as follows:
 *
 * <pre>{@code
 * -- setInitialState -> provides state of all operators in the chain
 *
 * -- invoke()
 *       |
 *       +----> Create basic utils (config, etc) and load the chain of operators
 *       +----> operators.setup()
 *       +----> task specific init()
 *       +----> initialize-operator-states()
 *       +----> open-operators()
 *       +----> run()
 *       +----> finish-operators()
 *       +----> close-operators()
 *       +----> common cleanup
 *       +----> task specific cleanup()
 * }</pre>
 *
 * <p>The {@code StreamTask} has a lock object called {@code lock}. All calls to methods on a {@code
 * StreamOperator} must be synchronized on this lock object to ensure that no methods are called
 * concurrently.
 *
 * @param <OUT>
 * @param <OP>
 */
//所有流任务的基类。
// 任务是由 TaskManager 部署和执行的本地处理单元。
// 每个任务都运行一个或多个StreamOperator ，这些 StreamOperator 形成任务的运算符链。
// 链接在一起的运算符在同一线程中同步执行，因此在同一流分区上同步执行。这些链的常见情况是连续的地图/ 平面地图/ 过滤器任务。
//任务链包含一个“头”操作符和多个链式操作符。
// StreamTask 专门用于头运算符的类型：一输入和二输入任务，以及源、迭代头和迭代尾。
//Task 类处理头操作符读取的流的设置，以及操作符链末端操作符生成的流。
// 请注意，链条可能分叉，因此具有多个末端。
//任务的生命周期设置如下：

//StreamTask有一个名为lock的锁对象。
// 对StreamOperator上的方法的所有调用都必须在此锁对象上同步，以确保不会同时调用任何方法。
@Internal
public abstract class StreamTask<OUT, OP extends StreamOperator<OUT>>
        implements TaskInvokable,
                CheckpointableTask,
                CoordinatedTask,
                AsyncExceptionHandler,
                ContainingTaskDetails {

    /** The thread group that holds all trigger timer threads. */
    //保存所有触发定时器线程的线程组。
    public static final ThreadGroup TRIGGER_THREAD_GROUP = new ThreadGroup("Triggers");

    /** The logger used by the StreamTask and its subclasses. */
    //StreamTask 及其子类使用的记录器。
    protected static final Logger LOG = LoggerFactory.getLogger(StreamTask.class);

    // ------------------------------------------------------------------------

    /**
     * All actions outside of the task {@link #mailboxProcessor mailbox} (i.e. performed by another
     * thread) must be executed through this executor to ensure that we don't have concurrent method
     * calls that void consistent checkpoints. The execution will always be performed in the task
     * thread.
     *
     * <p>CheckpointLock is superseded by {@link MailboxExecutor}, with {@link
     * StreamTaskActionExecutor.SynchronizedStreamTaskActionExecutor
     * SynchronizedStreamTaskActionExecutor} to provide lock to {@link SourceStreamTask}.
     */
    //任务mailbox之外的所有操作（即由另一个线程执行）必须通过此执行器执行，
    // 以确保我们不会有导致一致性检查点失效的并发方法调用。执行将始终在任务线程中执行。
    //CheckpointLock 被MailboxExecutor取代，
    // 并使用SynchronizedStreamTaskActionExecutor为SourceStreamTask提供锁定。
    private final StreamTaskActionExecutor actionExecutor;

    /** The input processor. Initialized in {@link #init()} method. */
    //输入处理器。在init()方法中初始化。
    @Nullable protected StreamInputProcessor inputProcessor;

    /** the main operator that consumes the input streams of this task. */
    //消耗该任务的输入流的主运算符。
    protected OP mainOperator;

    /** The chain of operators executed by this task. */
    //此任务执行的运算符链。
    protected OperatorChain<OUT, OP> operatorChain;

    /** The configuration of this streaming task. */
    //此流任务的配置。
    protected final StreamConfig configuration;

    /** Our state backend. We use this to create a keyed state backend. */
    //我们的状态后端。我们用它来创建一个键控状态后端。
    protected final StateBackend stateBackend;

    /** Our checkpoint storage. We use this to create checkpoint streams. */
    //我们的检查点存储。我们用它来创建检查点流。
    protected final CheckpointStorage checkpointStorage;

    private final SubtaskCheckpointCoordinator subtaskCheckpointCoordinator;

    /**
     * The internal {@link TimerService} used to define the current processing time (default =
     * {@code System.currentTimeMillis()}) and register timers for tasks to be executed in the
     * future.
     */
    //内部TimerService用于定义当前处理时间（默认 = System. currentTimeMillis() ）并为将来要执行的任务注册计时器。
    protected final TimerService timerService;

    /**
     * In contrast to {@link #timerService} we should not register any user timers here. It should
     * be used only for system level timers.
     */
    //与timerService相比，我们不应该在这里注册任何用户计时器。它应该仅用于系统级计时器。
    protected final TimerService systemTimerService;

    /** The currently active background materialization threads. */
    //当前活动的后台物化线程。
    private final CloseableRegistry cancelables = new CloseableRegistry();

    private final AutoCloseableRegistry resourceCloser;

    private final StreamTaskAsyncExceptionHandler asyncExceptionHandler;

    /**
     * Flag to mark the task "in operation", in which case check needs to be initialized to true, so
     * that early cancel() before invoke() behaves correctly.
     */
    //用于标记任务“正在运行”的标志，在这种情况下，
    // 需要将 check 初始化为 true，以便在 invoke() 之前提前 cancel() 行为正确
    private volatile boolean isRunning;

    /** Flag to mark the task at restoring duration in {@link #restore()}. */
    //用于在restore()中标记恢复持续时间的任务的标志。
    private volatile boolean isRestoring;

    /** Flag to mark this task as canceled. */
    //将此任务标记为已取消的标记。
    private volatile boolean canceled;

    /**
     * Flag to mark this task as failing, i.e. if an exception has occurred inside {@link
     * #invoke()}.
     */
    //将此任务标记为失败的标志，即，如果invoke()内部发生异常
    private volatile boolean failing;

    /** Flags indicating the finished method of all the operators are called. */
    //指示调用所有运算符的完成方法的标志。
    private boolean finishedOperators;

    private boolean closedOperators;

    /** Thread pool for async snapshot workers. */
    //异步快照工作人员的线程池。
    private final ExecutorService asyncOperationsThreadPool;

    protected final RecordWriterDelegate<SerializationDelegate<StreamRecord<OUT>>> recordWriter;

    protected final MailboxProcessor mailboxProcessor;

    final MailboxExecutor mainMailboxExecutor;

    /** TODO it might be replaced by the global IO executor on TaskManager level future. */
    //未来可能会被 TaskManager 级别的全局 IO 执行器取代。
    private final ExecutorService channelIOExecutor;

    // ========================================================
    //  Final  checkpoint / savepoint
    // ========================================================
    private Long syncSavepoint = null;
    private Long finalCheckpointMinId = null;
    private final CompletableFuture<Void> finalCheckpointCompleted = new CompletableFuture<>();

    private long latestReportCheckpointId = -1;

    private long latestAsyncCheckpointStartDelayNanos;

    private volatile boolean endOfDataReceived = false;

    private final long bufferDebloatPeriod;

    private final Environment environment;

    private final Object shouldInterruptOnCancelLock = new Object();

    @GuardedBy("shouldInterruptOnCancelLock")
    private boolean shouldInterruptOnCancel = true;

    @Nullable private final AvailabilityProvider changelogWriterAvailabilityProvider;

    private long initializeStateEndTs;

    // ------------------------------------------------------------------------

    /**
     * Constructor for initialization, possibly with initial state (recovery / savepoint / etc).
     *
     * @param env The task environment for this task.
     */
    //用于初始化的构造函数，可能具有初始状态 (恢复/ 保存点/ 等)。
    //参数:
    //env-此任务的任务环境。
    protected StreamTask(Environment env) throws Exception {
        this(env, null);
    }

    /**
     * Constructor for initialization, possibly with initial state (recovery / savepoint / etc).
     *
     * @param env The task environment for this task.
     * @param timerService Optionally, a specific timer service to use.
     */
    //用于初始化的构造函数，可能具有初始状态 (恢复/ 保存点/ 等)。
    //参数:
    //env -此任务的任务环境。
    //timerService -(可选) 要使用的特定计时器服务。
    protected StreamTask(Environment env, @Nullable TimerService timerService) throws Exception {
        this(env, timerService, FatalExitExceptionHandler.INSTANCE);
    }

    protected StreamTask(
            Environment environment,
            @Nullable TimerService timerService,
            Thread.UncaughtExceptionHandler uncaughtExceptionHandler)
            throws Exception {
        this(
                environment,
                timerService,
                uncaughtExceptionHandler,
                StreamTaskActionExecutor.IMMEDIATE);
    }

    /**
     * Constructor for initialization, possibly with initial state (recovery / savepoint / etc).
     *
     * <p>This constructor accepts a special {@link TimerService}. By default (and if null is passes
     * for the timer service) a {@link SystemProcessingTimeService DefaultTimerService} will be
     * used.
     *
     * @param environment The task environment for this task.
     * @param timerService Optionally, a specific timer service to use.
     * @param uncaughtExceptionHandler to handle uncaught exceptions in the async operations thread
     *     pool
     * @param actionExecutor a mean to wrap all actions performed by this task thread. Currently,
     *     only SynchronizedActionExecutor can be used to preserve locking semantics.
     */
    //用于初始化的构造函数，可能具有初始状态 (恢复/ 保存点/ 等)。
    //此构造函数接受一个特殊的TimerService。默认情况下 (如果为计时器服务传递null)，将使用DefaultTimerService。
    //参数:
    //environment -此任务的任务环境。
    //timerService -(可选) 要使用的特定计时器服务。
    //uncaughtExceptionHandler- 处理异步操作线程池中的未捕获异常
    //actionExecutor -用于包装此任务线程执行的所有操作。目前，只有SynchronizedActionExecutor可用于保留锁定语义。
    protected StreamTask(
            Environment environment,
            @Nullable TimerService timerService,
            Thread.UncaughtExceptionHandler uncaughtExceptionHandler,
            StreamTaskActionExecutor actionExecutor)
            throws Exception {
        this(
                environment,
                timerService,
                uncaughtExceptionHandler,
                actionExecutor,
                new TaskMailboxImpl(Thread.currentThread()));
    }

    protected StreamTask(
            Environment environment,
            @Nullable TimerService timerService,
            Thread.UncaughtExceptionHandler uncaughtExceptionHandler,
            StreamTaskActionExecutor actionExecutor,
            TaskMailbox mailbox)
            throws Exception {
        // The registration of all closeable resources. The order of registration is important.
        //所有可关闭资源的注册。注册的顺序很重要。
        resourceCloser = new AutoCloseableRegistry();
        try {
            this.environment = environment;
            this.configuration = new StreamConfig(environment.getTaskConfiguration());

            // Initialize mailbox metrics
            //初始化mailbox指标
            MailboxMetricsController mailboxMetricsControl =
                    new MailboxMetricsController(
                            environment.getMetricGroup().getIOMetricGroup().getMailboxLatency(),
                            environment
                                    .getMetricGroup()
                                    .getIOMetricGroup()
                                    .getNumMailsProcessedCounter());
            environment
                    .getMetricGroup()
                    .getIOMetricGroup()
                    .registerMailboxSizeSupplier(() -> mailbox.size());

            //在Flink应用执行数据处理、checkpoint执行、定时器触发等过程中可能会同时修改状态，
            //Flink系统通过引入Mailbox线程模型来解决状态操作不一致的情况。
            //其中MailboxProcessor负责拉取、处理Mailbox中的Mail，即checkpoint执行、定时器触发等动作，
            //而MailboxProcessor成员变量MailboxDefaultAction mailboxDefaultAction
            //默认动作负责DataStream上普通消息的处理，包括：处理Event、barrier、Watermark等。
            //TaskMailboxImpl为Mailbox的实现，负责存储checkpoint执行、定时器触发等动作，
            //MailboxExecutorImpl负责提交生成checkpoint执行、定时器触发等动作。
            this.mailboxProcessor =
                    new MailboxProcessor(
                            //processInput()方法作为成员mailboxDefaultAction的值，负责常规的数据处理
                            this::processInput,
                            mailbox, actionExecutor, mailboxMetricsControl);

            // Should be closed last.
            // 应该最后关闭。
            resourceCloser.registerCloseable(mailboxProcessor);

            this.channelIOExecutor =
                    MdcUtils.scopeToJob(
                            environment.getJobID(),
                            Executors.newSingleThreadExecutor(
                                    new ExecutorThreadFactory("channel-state-unspilling")));
            resourceCloser.registerCloseable(channelIOExecutor::shutdown);

            //RecordWriter创建入口。
            this.recordWriter = createRecordWriterDelegate(configuration, environment);
            // Release the output resources. this method should never fail.
            // 释放输出资源。这个方法永远不会失败。
            resourceCloser.registerCloseable(this::releaseOutputResources);
            // If the operators won't be closed explicitly, register it to a hard close.
            // 如果运算符不会显式关闭，请将其注册为硬关闭。
            resourceCloser.registerCloseable(this::closeAllOperators);
            resourceCloser.registerCloseable(this::cleanUpInternal);

            this.actionExecutor = Preconditions.checkNotNull(actionExecutor);
            this.mainMailboxExecutor = mailboxProcessor.getMainMailboxExecutor();
            this.asyncExceptionHandler = new StreamTaskAsyncExceptionHandler(environment);

            // With maxConcurrentCheckpoints + 1 we more or less adhere to the
            // maxConcurrentCheckpoints configuration, but allow for a small leeway with allowing
            // for simultaneous N ongoing concurrent checkpoints and for example clean up of one
            // aborted one.
            //使用 maxConcurrentCheckpoints + 1，我们或多或少地遵循 maxConcurrentCheckpoints 配置，
            // 但允许有一个小的余地，允许同时进行 N 个正在进行的并发检查点，例如清理一个中止的检查点。
            this.asyncOperationsThreadPool =
                    MdcUtils.scopeToJob(
                            getEnvironment().getJobID(),
                            new ThreadPoolExecutor(
                                    0,
                                    configuration.getMaxConcurrentCheckpoints() + 1,
                                    60L,
                                    TimeUnit.SECONDS,
                                    new LinkedBlockingQueue<>(),
                                    new ExecutorThreadFactory(
                                            "AsyncOperations", uncaughtExceptionHandler)));

            // Register all asynchronous checkpoint threads.
            //注册所有异步检查点线程。
            resourceCloser.registerCloseable(this::shutdownAsyncThreads);
            resourceCloser.registerCloseable(cancelables);

            environment.setMainMailboxExecutor(mainMailboxExecutor);
            environment.setAsyncOperationsThreadPool(asyncOperationsThreadPool);

            //StateBackend实例创建过程
            this.stateBackend = createStateBackend();
            this.checkpointStorage = createCheckpointStorage(stateBackend);
            this.changelogWriterAvailabilityProvider =
                    environment.getTaskStateManager().getStateChangelogStorage() == null
                            ? null
                            : environment
                                    .getTaskStateManager()
                                    .getStateChangelogStorage()
                                    .getAvailabilityProvider();

            CheckpointStorageAccess checkpointStorageAccess =
                    checkpointStorage.createCheckpointStorage(getEnvironment().getJobID());
            checkpointStorageAccess =
                    tryApplyFileMergingCheckpoint(
                            checkpointStorageAccess,
                            environment.getTaskStateManager().getFileMergingSnapshotManager());
            environment.setCheckpointStorageAccess(checkpointStorageAccess);

            // if the clock is not already set, then assign a default TimeServiceProvider
            // 如果尚未设置时钟，则分配默认的 TimeServiceProvider
            if (timerService == null) {
                this.timerService = createTimerService("Time Trigger for " + getName());
            } else {
                this.timerService = timerService;
            }

            this.systemTimerService = createTimerService("System Time Trigger for " + getName());
            final CheckpointStorageAccess finalCheckpointStorageAccess = checkpointStorageAccess;

            ChannelStateWriter channelStateWriter =
                    configuration.isUnalignedCheckpointsEnabled()
                            ? openChannelStateWriter(
                                    getName(),
                                    // Note: don't pass checkpointStorageAccess directly to channel
                                    // state writer.
                                    // The fileSystem of checkpointStorageAccess may be an instance
                                    // of SafetyNetWrapperFileSystem, which close all held streams
                                    // when thread exits. Channel state writers are invoked in other
                                    // threads instead of task thread, therefore channel state
                                    // writer cannot share file streams directly, otherwise
                                    // conflicts will occur on job exit.
                            //注意：不要将 checkpointStorageAccess 直接传递给通道状态编写器。
                            // checkpointStorageAccess 的文件系统可以是 SafetyNetWrapperFileSystem 的实例，
                            // 它在线程退出时关闭所有持有的流。
                            // 通道状态写入器是在其他线程而不是任务线程中调用的，
                            // 因此通道状态写入器不能直接共享文件流，否则作业退出时会发生冲突。
                                    () -> {
                                        if (finalCheckpointStorageAccess
                                                instanceof FsMergingCheckpointStorageAccess) {
                                            // FsMergingCheckpointStorageAccess using unguarded
                                            // fileSystem, which can be shared.
                                            //FsMergingCheckpointStorageAccess 使用不受保护的文件系统，可以共享。
                                            return finalCheckpointStorageAccess;
                                        } else {
                                            // Other checkpoint storage access should be lazily
                                            // initialized to avoid sharing.
                                            //其他检查点存储访问应延迟初始化以避免共享。
                                            return checkpointStorage.createCheckpointStorage(
                                                    getEnvironment().getJobID());
                                        }
                                    },
                                    environment,
                                    configuration.getMaxSubtasksPerChannelStateFile())
                            : ChannelStateWriter.NO_OP;
            this.subtaskCheckpointCoordinator =
                    new SubtaskCheckpointCoordinatorImpl(
                            checkpointStorageAccess,
                            getName(),
                            actionExecutor,
                            getAsyncOperationsThreadPool(),
                            environment,
                            this,
                            this::prepareInputSnapshot,
                            configuration.getMaxConcurrentCheckpoints(),
                            channelStateWriter,
                            configuration
                                    .getConfiguration()
                                    .get(
                                            CheckpointingOptions
                                                    .ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH),
                            BarrierAlignmentUtil.createRegisterTimerCallback(
                                    mainMailboxExecutor, systemTimerService),
                            environment.getTaskStateManager().getFileMergingSnapshotManager());
            resourceCloser.registerCloseable(subtaskCheckpointCoordinator::close);

            // Register to stop all timers and threads. Should be closed first.
            //注册以停止所有计时器和线程。应该先关闭。
            resourceCloser.registerCloseable(this::tryShutdownTimerService);

            injectChannelStateWriterIntoChannels();

            environment.getMetricGroup().getIOMetricGroup().setEnableBusyTime(true);
            Configuration taskManagerConf = environment.getTaskManagerInfo().getConfiguration();

            this.bufferDebloatPeriod = taskManagerConf.get(BUFFER_DEBLOAT_PERIOD).toMillis();
            mailboxMetricsControl.setupLatencyMeasurement(systemTimerService, mainMailboxExecutor);
        } catch (Exception ex) {
            try {
                resourceCloser.close();
            } catch (Throwable throwable) {
                ex.addSuppressed(throwable);
            }
            throw ex;
        }
    }

    private CheckpointStorageAccess tryApplyFileMergingCheckpoint(
            CheckpointStorageAccess checkpointStorageAccess,
            @Nullable FileMergingSnapshotManager fileMergingSnapshotManager) {
        if (fileMergingSnapshotManager == null) {
            return checkpointStorageAccess;
        }
        try {
            CheckpointStorageAccess mergingCheckpointStorageAccess =
                    (CheckpointStorageAccess)
                            checkpointStorageAccess.toFileMergingStorage(
                                    fileMergingSnapshotManager, environment);
            mergingCheckpointStorageAccess.initializeBaseLocationsForCheckpoint();
            if (mergingCheckpointStorageAccess instanceof FsMergingCheckpointStorageAccess) {
                resourceCloser.registerCloseable(
                        () ->
                                ((FsMergingCheckpointStorageAccess) mergingCheckpointStorageAccess)
                                        .close());
            }
            return mergingCheckpointStorageAccess;
        } catch (IOException e) {
            LOG.warn(
                    "Initiating FsMergingCheckpointStorageAccess failed "
                            + "with exception: {}, falling back to original checkpoint storage access {}.",
                    e.getMessage(),
                    checkpointStorageAccess.getClass(),
                    e);
            return checkpointStorageAccess;
        }
    }

    private TimerService createTimerService(String timerThreadName) {
        ThreadFactory timerThreadFactory =
                new DispatcherThreadFactory(TRIGGER_THREAD_GROUP, timerThreadName);
        return new SystemProcessingTimeService(this::handleTimerException, timerThreadFactory);
    }

    private void injectChannelStateWriterIntoChannels() {
        final Environment env = getEnvironment();
        final ChannelStateWriter channelStateWriter =
                subtaskCheckpointCoordinator.getChannelStateWriter();
        for (final InputGate gate : env.getAllInputGates()) {
            gate.setChannelStateWriter(channelStateWriter);
        }
        for (ResultPartitionWriter writer : env.getAllWriters()) {
            if (writer instanceof ChannelStateHolder) {
                ((ChannelStateHolder) writer).setChannelStateWriter(channelStateWriter);
            }
        }
    }

    private CompletableFuture<Void> prepareInputSnapshot(
            ChannelStateWriter channelStateWriter, long checkpointId) throws CheckpointException {
        if (inputProcessor == null) {
            return FutureUtils.completedVoidFuture();
        }
        return inputProcessor.prepareSnapshot(channelStateWriter, checkpointId);
    }

    SubtaskCheckpointCoordinator getCheckpointCoordinator() {
        return subtaskCheckpointCoordinator;
    }

    // ------------------------------------------------------------------------
    //  Life cycle methods for specific implementations
    // ------------------------------------------------------------------------

    protected abstract void init() throws Exception;

    protected void cancelTask() throws Exception {}

    /**
     * This method implements the default action of the task (e.g. processing one event from the
     * input). Implementations should (in general) be non-blocking.
     *
     * @param controller controller object for collaborative interaction between the action and the
     *     stream task.
     * @throws Exception on any problems in the action.
     */
    //此方法实现任务的默认操作 (例如，处理来自输入的一个事件)。实现应该 (通常) 是非阻塞的。
    protected void processInput(MailboxDefaultAction.Controller controller) throws Exception {
        //过程输入
        DataInputStatus status = inputProcessor.processInput();
        switch (status) {
            case MORE_AVAILABLE:
                if (taskIsAvailable()) {
                    return;
                }
                break;
            case NOTHING_AVAILABLE:
                break;
            case END_OF_RECOVERY:
                throw new IllegalStateException("We should not receive this event here.");
            case STOPPED:
                endData(StopMode.NO_DRAIN);
                return;
            case END_OF_DATA:
                endData(StopMode.DRAIN);
                notifyEndOfData();
                return;
            case END_OF_INPUT:
                // Suspend the mailbox processor, it would be resumed in afterInvoke and finished
                // after all records processed by the downstream tasks. We also suspend the default
                // actions to avoid repeat executing the empty default operation (namely process
                // records).
                controller.suspendDefaultAction();
                mailboxProcessor.suspend();
                return;
        }

        TaskIOMetricGroup ioMetrics = getEnvironment().getMetricGroup().getIOMetricGroup();
        PeriodTimer timer;
        CompletableFuture<?> resumeFuture;
        if (!recordWriter.isAvailable()) {
            timer = new GaugePeriodTimer(ioMetrics.getSoftBackPressuredTimePerSecond());
            resumeFuture = recordWriter.getAvailableFuture();
        } else if (!inputProcessor.isAvailable()) {
            timer = new GaugePeriodTimer(ioMetrics.getIdleTimeMsPerSecond());
            resumeFuture = inputProcessor.getAvailableFuture();
        } else if (changelogWriterAvailabilityProvider != null
                && !changelogWriterAvailabilityProvider.isAvailable()) {
            // waiting for changelog availability is reported as busy
            //等待变更日志可用性被报告为繁忙
            timer = new GaugePeriodTimer(ioMetrics.getChangelogBusyTimeMsPerSecond());
            resumeFuture = changelogWriterAvailabilityProvider.getAvailableFuture();
        } else {
            // data availability has changed in the meantime; retry immediately
            // 数据可用性同时发生了变化；立即重试
            return;
        }
        assertNoException(
                resumeFuture.thenRun(
                        new ResumeWrapper(controller.suspendDefaultAction(timer), timer)));
    }

    protected void endData(StopMode mode) throws Exception {

        if (mode == StopMode.DRAIN) {
            advanceToEndOfEventTime();
        }
        // finish all operators in a chain effect way
        // 以连锁效应的方式完成所有操作员
        operatorChain.finishOperators(actionExecutor, mode);
        this.finishedOperators = true;

        for (ResultPartitionWriter partitionWriter : getEnvironment().getAllWriters()) {
            partitionWriter.notifyEndOfData(mode);
        }

        this.endOfDataReceived = true;
    }

    protected void notifyEndOfData() {
        environment.getTaskManagerActions().notifyEndOfData(environment.getExecutionId());
    }

    protected void setSynchronousSavepoint(long checkpointId) {
        checkState(
                syncSavepoint == null || syncSavepoint == checkpointId,
                "at most one stop-with-savepoint checkpoint at a time is allowed");
        syncSavepoint = checkpointId;
    }

    @VisibleForTesting
    OptionalLong getSynchronousSavepointId() {
        if (syncSavepoint != null) {
            return OptionalLong.of(syncSavepoint);
        } else {
            return OptionalLong.empty();
        }
    }

    private boolean isCurrentSyncSavepoint(long checkpointId) {
        return syncSavepoint != null && syncSavepoint == checkpointId;
    }

    /**
     * Emits the {@link org.apache.flink.streaming.api.watermark.Watermark#MAX_WATERMARK
     * MAX_WATERMARK} so that all registered timers are fired.
     *
     * <p>This is used by the source task when the job is {@code TERMINATED}. In the case, we want
     * all the timers registered throughout the pipeline to fire and the related state (e.g.
     * windows) to be flushed.
     *
     * <p>For tasks other than the source task, this method does nothing.
     */
    //发出MAX_WATERMARK以便触发所有注册的计时器。
    //当作业TERMINATED时，源任务将使用它。
    // 在这种情况下，我们希望在整个管道中注册的所有计时器都被触发，并且相关状态（例如窗口）被刷新。
    //对于源任务以外的任务，此方法不执行任何操作。
    protected void advanceToEndOfEventTime() throws Exception {}

    // ------------------------------------------------------------------------
    //  Core work methods of the Stream Task
    // ------------------------------------------------------------------------

    public StreamTaskStateInitializer createStreamTaskStateInitializer(
            SubTaskInitializationMetricsBuilder initializationMetrics) {
        InternalTimeServiceManager.Provider timerServiceProvider =
                configuration.getTimerServiceProvider(getUserCodeClassLoader());
        return new StreamTaskStateInitializerImpl(
                getEnvironment(),
                stateBackend,
                initializationMetrics,
                TtlTimeProvider.DEFAULT,
                timerServiceProvider != null
                        ? timerServiceProvider
                        : InternalTimeServiceManagerImpl::create,
                () -> canceled);
    }

    protected Counter setupNumRecordsInCounter(StreamOperator streamOperator) {
        try {
            return streamOperator.getMetricGroup().getIOMetricGroup().getNumRecordsInCounter();
        } catch (Exception e) {
            LOG.warn("An exception occurred during the metrics setup.", e);
            return new SimpleCounter();
        }
    }

    @Override
    public final void restore() throws Exception {
        restoreInternal();
    }

    void restoreInternal() throws Exception {
        if (isRunning) {
            LOG.debug("Re-restore attempt rejected.");
            return;
        }
        isRestoring = true;
        closedOperators = false;
        getEnvironment().getMetricGroup().getIOMetricGroup().markTaskInitializationStarted();
        LOG.debug("Initializing {}.", getName());

        SubTaskInitializationMetricsBuilder initializationMetrics =
                new SubTaskInitializationMetricsBuilder(
                        SystemClock.getInstance().absoluteTimeMillis());
        try {
            operatorChain =
                    getEnvironment().getTaskStateManager().isTaskDeployedAsFinished()
                            ? new FinishedOperatorChain<>(this, recordWriter)
                            //创建OperatorChain实例，一般来说OperatorChain都是RegularOperatorChain类型，
                            //转入到RegularOperatorChain构造函数中。
                            : new RegularOperatorChain<>(this, recordWriter);
            mainOperator = operatorChain.getMainOperator();

            getEnvironment()
                    .getTaskStateManager()
                    .getRestoreCheckpointId()
                    .ifPresent(restoreId -> latestReportCheckpointId = restoreId);

            // task specific initialization
            //特定于任务的初始化
            //该方法主要由子类实现。常见的StreamTask子类包括OneInputStreamTask和SourceStreamTask等。
            init();
            configuration.clearInitialConfigs();

            // save the work of reloading state, etc, if the task is already canceled
            //如果任务已取消，则保存重新加载状态等的工作
            ensureNotCanceled();

            // -------- Invoke --------
            LOG.debug("Invoking {}", getName());

            // we need to make sure that any triggers scheduled in open() cannot be
            // executed before all operators are opened
            //我们需要确保在打开所有运算符之前，无法执行open() 中计划的任何触发器
            CompletableFuture<Void> allGatesRecoveredFuture =
                    //在该方法中有一步重要的操作就是初始化算子链中的各个算子。
                    actionExecutor.call(() -> restoreStateAndGates(initializationMetrics));

            // Run mailbox until all gates will be recovered.
            //其主要作用是触发mailboxDefaultAction默认动作的执行，即StreamTask.processInput(...)方法的执行。
            mailboxProcessor.runMailboxLoop();

            initializationMetrics.addDurationMetric(
                    GATE_RESTORE_DURATION,
                    SystemClock.getInstance().absoluteTimeMillis() - initializeStateEndTs);

            ensureNotCanceled();

            checkState(
                    allGatesRecoveredFuture.isDone(),
                    "Mailbox loop interrupted before recovery was finished.");

            // we recovered all the gates, we can close the channel IO executor as it is no longer
            // needed
            // 我们恢复了所有的gate，我们可以关闭通道 IO 执行器，因为它不再需要了
            channelIOExecutor.shutdown();

            isRunning = true;
            isRestoring = false;
            initializationMetrics.setStatus(InitializationStatus.COMPLETED);
        } finally {
            environment
                    .getTaskStateManager()
                    .reportInitializationMetrics(initializationMetrics.build());
        }
    }

    private CompletableFuture<Void> restoreStateAndGates(
            SubTaskInitializationMetricsBuilder initializationMetrics) throws Exception {

        long mailboxStartTs = SystemClock.getInstance().absoluteTimeMillis();
        initializationMetrics.addDurationMetric(
                MAILBOX_START_DURATION,
                mailboxStartTs - initializationMetrics.getInitializationStartTs());

        SequentialChannelStateReader reader =
                getEnvironment().getTaskStateManager().getSequentialChannelStateReader();
        reader.readOutputData(
                getEnvironment().getAllWriters(), !configuration.isGraphContainingLoops());

        long readOutputDataTs = SystemClock.getInstance().absoluteTimeMillis();
        initializationMetrics.addDurationMetric(
                READ_OUTPUT_DATA_DURATION, readOutputDataTs - mailboxStartTs);

        //调用算子链实例的initializeStateAndOpenOperators(...)方法。
        operatorChain.initializeStateAndOpenOperators(
                createStreamTaskStateInitializer(initializationMetrics));

        initializeStateEndTs = SystemClock.getInstance().absoluteTimeMillis();
        initializationMetrics.addDurationMetric(
                INITIALIZE_STATE_DURATION, initializeStateEndTs - readOutputDataTs);
        IndexedInputGate[] inputGates = getEnvironment().getAllInputGates();

        channelIOExecutor.execute(
                () -> {
                    try {
                        //读取输入数据
                        reader.readInputData(inputGates);
                    } catch (Exception e) {
                        asyncExceptionHandler.handleAsyncException(
                                "Unable to read channel state", e);
                    }
                });

        // We wait for all input channel state to recover before we go into RUNNING state, and thus
        // start checkpointing. If we implement incremental checkpointing of input channel state
        // we must make sure it supports CheckpointType#FULL_CHECKPOINT
        //在进入 RUNNING 状态之前，我们等待所有输入通道状态恢复，从而开始检查点。
        // 如果我们实现输入通道状态的增量检查点，我们必须确保它支持 CheckpointTypeFULL_CHECKPOINT
        List<CompletableFuture<?>> recoveredFutures = new ArrayList<>(inputGates.length);
        for (InputGate inputGate : inputGates) {
            recoveredFutures.add(inputGate.getStateConsumedFuture());

            inputGate
                    .getStateConsumedFuture()
                    .thenRun(
                            () ->
                                    mainMailboxExecutor.execute(
                                            //请求数据操作。
                                            inputGate::requestPartitions,
                                            "Input gate request partitions"));
        }

        return CompletableFuture.allOf(recoveredFutures.toArray(new CompletableFuture[0]))
                .thenRun(mailboxProcessor::suspend);
    }

    private void ensureNotCanceled() {
        if (canceled) {
            throw new CancelTaskException();
        }
    }

    @Override
    public final void invoke() throws Exception {
        // Allow invoking method 'invoke' without having to call 'restore' before it.
        // 允许调用方法“invoke”，而无需在其之前调用“restore”。
        if (!isRunning) {
            LOG.debug("Restoring during invoke will be called.");
            restoreInternal();
        }

        // final check to exit early before starting to run
        // 在开始运行之前进行最后的检查以提前退出
        ensureNotCanceled();

        scheduleBufferDebloater();

        // let the task do its work
        // 让任务发挥作用
        getEnvironment().getMetricGroup().getIOMetricGroup().markTaskStart();
        runMailboxLoop();

        // if this left the run() method cleanly despite the fact that this was canceled,
        // make sure the "clean shutdown" is not attempted
        // 如果尽管这已被取消，但仍干净地离开了 run() 方法，请确保不会尝试“干净关闭”
        ensureNotCanceled();

        afterInvoke();
    }

    private void scheduleBufferDebloater() {
        // See https://issues.apache.org/jira/browse/FLINK-23560
        // If there are no input gates, there is no point of calculating the throughput and running
        // the debloater. At the same time, for SourceStreamTask using legacy sources and checkpoint
        // lock, enqueuing even a single mailbox action can cause performance regression. This is
        // especially visible in batch, with disabled checkpointing and no processing time timers.
        //如果没有输入gate，则计算吞吐量和运行去膨胀器就没有意义。
        // 同时，对于使用旧源和检查点锁的 SourceStreamTask，即使将单个mailbox操作排队也可能会导致性能下降。
        // 这在禁用检查点且没有处理时间计时器的批处理中尤其明显。
        if (getEnvironment().getAllInputGates().length == 0
                || !environment
                        .getTaskManagerInfo()
                        .getConfiguration()
                        .get(TaskManagerOptions.BUFFER_DEBLOAT_ENABLED)) {
            return;
        }
        systemTimerService.registerTimer(
                systemTimerService.getCurrentProcessingTime() + bufferDebloatPeriod,
                timestamp ->
                        mainMailboxExecutor.execute(
                                () -> {
                                    debloat();
                                    scheduleBufferDebloater();
                                },
                                "Buffer size recalculation"));
    }

    @VisibleForTesting
    void debloat() {
        for (IndexedInputGate inputGate : environment.getAllInputGates()) {
            inputGate.triggerDebloating();
        }
    }

    @VisibleForTesting
    public boolean runSingleMailboxLoop() throws Exception {
        return mailboxProcessor.runSingleMailboxLoop();
    }

    @VisibleForTesting
    public boolean runMailboxStep() throws Exception {
        return mailboxProcessor.runMailboxStep();
    }

    @VisibleForTesting
    public boolean isMailboxLoopRunning() {
        return mailboxProcessor.isMailboxLoopRunning();
    }

    public void runMailboxLoop() throws Exception {
        mailboxProcessor.runMailboxLoop();
    }

    protected void afterInvoke() throws Exception {
        LOG.debug("Finished task {}", getName());
        getCompletionFuture().exceptionally(unused -> null).join();

        Set<CompletableFuture<Void>> terminationConditions = new HashSet<>();
        // If checkpoints are enabled, waits for all the records get processed by the downstream
        // tasks. During this process, this task could coordinate with its downstream tasks to
        // continue perform checkpoints.
        //如果启用检查点，则等待下游任务处理所有记录。在此过程中，该任务可以与其下游任务协调以继续执行检查点。
        if (endOfDataReceived && areCheckpointsWithFinishedTasksEnabled()) {
            LOG.debug("Waiting for all the records processed by the downstream tasks.");

            for (ResultPartitionWriter partitionWriter : getEnvironment().getAllWriters()) {
                terminationConditions.add(partitionWriter.getAllDataProcessedFuture());
            }

            terminationConditions.add(finalCheckpointCompleted);
        }

        if (syncSavepoint != null) {
            terminationConditions.add(finalCheckpointCompleted);
        }

        FutureUtils.waitForAll(terminationConditions)
                .thenRun(mailboxProcessor::allActionsCompleted);

        // Resumes the mailbox processor. The mailbox processor would be completed
        // after all records are processed by the downstream tasks.
        //恢复mailbox处理器。当下游任务处理完所有记录后，mailbox处理器将完成。
        mailboxProcessor.runMailboxLoop();

        // make sure no further checkpoint and notification actions happen.
        // at the same time, this makes sure that during any "regular" exit where still
        //确保不会发生进一步的检查点和通知操作。同时，这确保了在任何“常规”退出期间仍然
        actionExecutor.runThrowing(
                () -> {
                    // make sure no new timers can come
                    // 确保没有新的计时器到来
                    timerService.quiesce().get();
                    systemTimerService.quiesce().get();

                    // let mailbox execution reject all new letters from this point
                    // 让mailbox执行拒绝从此时起的所有新信件
                    mailboxProcessor.prepareClose();
                });

        // processes the remaining mails; no new mails can be enqueued
        //处理剩余mails；没有新mails可以入队
        mailboxProcessor.drain();

        // Set isRunning to false after all the mails are drained so that
        // the queued checkpoint requirements could be triggered normally.
        //当所有mails都被清空后，将 isRunning 设置为 false，以便可以正常触发排队的检查点要求。
        actionExecutor.runThrowing(
                () -> {
                    // only set the StreamTask to not running after all operators have been
                    // finished!
                    // See FLINK-7430
                    //仅在所有运算符完成后才将 StreamTask 设置为不运行！
                    isRunning = false;
                });

        LOG.debug("Finished operators for task {}", getName());

        // make sure all buffered data is flushed
        //确保所有缓冲数据都已刷新
        operatorChain.flushOutputs();

        if (areCheckpointsWithFinishedTasksEnabled()) {
            // No new checkpoints could be triggered since mailbox has been drained.
            // 由于mailbox已被清空，因此无法触发新的检查点。
            subtaskCheckpointCoordinator.waitForPendingCheckpoints();
            LOG.debug("All pending checkpoints are finished");
        }

        disableInterruptOnCancel();

        // make an attempt to dispose the operators such that failures in the dispose call
        // still let the computation fail
        // 尝试处置运算符，以便处置调用中的失败仍然导致计算失败
        closeAllOperators();
    }

    private boolean areCheckpointsWithFinishedTasksEnabled() {
        return configuration
                        .getConfiguration()
                        .get(CheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH)
                && configuration.isCheckpointingEnabled();
    }

    @Override
    public final void cleanUp(Throwable throwable) throws Exception {
        LOG.debug(
                "Cleanup StreamTask (operators closed: {}, cancelled: {})",
                closedOperators,
                canceled);

        failing = !canceled && throwable != null;

        Exception cancelException = null;
        if (throwable != null) {
            try {
                cancelTask();
            } catch (Throwable t) {
                cancelException = t instanceof Exception ? (Exception) t : new Exception(t);
            }
        }

        disableInterruptOnCancel();

        // note: This `.join()` is already uninterruptible, so it doesn't matter if we have already
        // disabled the interruptions or not.
        //注意：这个`.join()`已经是不可中断的，所以我们是否已经禁用中断并不重要。
        getCompletionFuture().exceptionally(unused -> null).join();
        // clean up everything we initialized
        // 清理我们初始化的所有内容
        isRunning = false;

        // clear any previously issued interrupt for a more graceful shutdown
        // 清除任何先前发出的中断以更正常地关闭
        Thread.interrupted();

        try {
            resourceCloser.close();
        } catch (Throwable t) {
            Exception e = t instanceof Exception ? (Exception) t : new Exception(t);
            throw firstOrSuppressed(e, cancelException);
        }
    }

    protected void cleanUpInternal() throws Exception {
        if (inputProcessor != null) {
            inputProcessor.close();
        }
    }

    protected CompletableFuture<Void> getCompletionFuture() {
        return FutureUtils.completedVoidFuture();
    }

    @Override
    public final void cancel() throws Exception {
        isRunning = false;
        canceled = true;

        FlinkSecurityManager.monitorUserSystemExitForCurrentThread();
        // the "cancel task" call must come first, but the cancelables must be
        // closed no matter what
        // “取消任务”调用必须首先进行，但无论如何都必须关闭可取消任务
        try {
            cancelTask();
        } finally {
            FlinkSecurityManager.unmonitorUserSystemExitForCurrentThread();
            getCompletionFuture()
                    .whenComplete(
                            (unusedResult, unusedError) -> {
                                // WARN: the method is called from the task thread but the callback
                                // can be invoked from a different thread
                                //警告：该方法是从任务线程调用的，但回调可以从不同的线程调用
                                mailboxProcessor.allActionsCompleted();
                                try {
                                    subtaskCheckpointCoordinator.cancel();
                                    cancelables.close();
                                } catch (IOException e) {
                                    throw new CompletionException(e);
                                }
                            });
        }
    }

    public MailboxExecutorFactory getMailboxExecutorFactory() {
        return this.mailboxProcessor::getMailboxExecutor;
    }

    public boolean hasMail() {
        return mailboxProcessor.hasMail();
    }

    private boolean taskIsAvailable() {
        return recordWriter.isAvailable()
                && (changelogWriterAvailabilityProvider == null
                        || changelogWriterAvailabilityProvider.isAvailable());
    }

    public CanEmitBatchOfRecordsChecker getCanEmitBatchOfRecords() {
        return () -> !this.mailboxProcessor.hasMail() && taskIsAvailable();
    }

    public final boolean isRunning() {
        return isRunning;
    }

    public final boolean isCanceled() {
        return canceled;
    }

    public final boolean isFailing() {
        return failing;
    }

    private void shutdownAsyncThreads() throws Exception {
        if (!asyncOperationsThreadPool.isShutdown()) {
            asyncOperationsThreadPool.shutdownNow();
        }
    }

    private void releaseOutputResources() throws Exception {
        if (operatorChain != null) {
            // beware: without synchronization, #performCheckpoint() may run in
            //         parallel and this call is not thread-safe
            // 注意：如果没有同步，performCheckpoint() 可能会并行运行，并且此调用不是线程安全的
            actionExecutor.run(() -> operatorChain.close());
        } else {
            // failed to allocate operatorChain, clean up record writers
            // 分配operatorChain失败，清理记录写入器
            recordWriter.close();
        }
    }

    /** Closes all the operators if not closed before. */
    //如果之前未关闭，则关闭所有运算符。
    private void closeAllOperators() throws Exception {
        if (operatorChain != null && !closedOperators) {
            closedOperators = true;
            operatorChain.closeAllOperators();
        }
    }

    /**
     * The finalize method shuts down the timer. This is a fail-safe shutdown, in case the original
     * shutdown method was never called.
     *
     * <p>This should not be relied upon! It will cause shutdown to happen much later than if manual
     * shutdown is attempted, and cause threads to linger for longer than needed.
     */
    //Finalize 方法关闭计时器。这是一种故障安全关闭，以防从未调用原始关闭方法。
    //这不应该被依赖！它会导致关闭比尝试手动关闭晚得多，并导致线程停留的时间超过所需的时间。
    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        if (!timerService.isTerminated()) {
            LOG.info("Timer service is shutting down.");
            timerService.shutdownService();
        }

        if (!systemTimerService.isTerminated()) {
            LOG.info("System timer service is shutting down.");
            systemTimerService.shutdownService();
        }

        cancelables.close();
    }

    boolean isSerializingTimestamps() {
        TimeCharacteristic tc = configuration.getTimeCharacteristic();
        return tc == TimeCharacteristic.EventTime | tc == TimeCharacteristic.IngestionTime;
    }

    // ------------------------------------------------------------------------
    //  Access to properties and utilities
    // ------------------------------------------------------------------------

    /**
     * Gets the name of the task, in the form "taskname (2/5)".
     *
     * @return The name of the task.
     */
    //获取任务的名称，格式为“taskname (2/ 5)”。
    //返回：
    //任务的名称。
    public final String getName() {
        return getEnvironment().getTaskInfo().getTaskNameWithSubtasks();
    }

    /**
     * Gets the name of the task, appended with the subtask indicator and execution id.
     *
     * @return The name of the task, with subtask indicator and execution id.
     */
    //获取任务名称，附加子任务指示符和执行 ID。
    //返回：
    //任务名称，带有子任务指示符和执行 ID。
    String getTaskNameWithSubtaskAndId() {
        return getEnvironment().getTaskInfo().getTaskNameWithSubtasks()
                + " ("
                + getEnvironment().getExecutionId()
                + ')';
    }

    public CheckpointStorageWorkerView getCheckpointStorage() {
        return subtaskCheckpointCoordinator.getCheckpointStorage();
    }

    public StreamConfig getConfiguration() {
        return configuration;
    }

    RecordWriterOutput<?>[] getStreamOutputs() {
        return operatorChain.getStreamOutputs();
    }

    // ------------------------------------------------------------------------
    //  Checkpoint and Restore
    // ------------------------------------------------------------------------

    @Override
    public CompletableFuture<Boolean> triggerCheckpointAsync(
            CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions) {
        checkForcedFullSnapshotSupport(checkpointOptions);

        CompletableFuture<Boolean> result = new CompletableFuture<>();
        mainMailboxExecutor.execute(
                () -> {
                    try {
                        boolean noUnfinishedInputGates =
                                Arrays.stream(getEnvironment().getAllInputGates())
                                        .allMatch(InputGate::isFinished);

                        if (noUnfinishedInputGates) {
                            result.complete(
                                    //触发器检查点异步
                                    triggerCheckpointAsyncInMailbox(
                                            checkpointMetaData, checkpointOptions));
                        } else {
                            result.complete(
                                    //触发未完成的通道检查点
                                    triggerUnfinishedChannelsCheckpoint(
                                            checkpointMetaData, checkpointOptions));
                        }
                    } catch (Exception ex) {
                        // Report the failure both via the Future result but also to the mailbox
                        //既可以通过 Future 结果报告失败，也可以通过mailbox报告失败
                        result.completeExceptionally(ex);
                        throw ex;
                    }
                },
                "checkpoint %s with %s",
                checkpointMetaData,
                checkpointOptions);
        return result;
    }

    private boolean triggerCheckpointAsyncInMailbox(
            CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions)
            throws Exception {
        FlinkSecurityManager.monitorUserSystemExitForCurrentThread();
        try {
            latestAsyncCheckpointStartDelayNanos =
                    1_000_000
                            * Math.max(
                                    0,
                                    System.currentTimeMillis() - checkpointMetaData.getTimestamp());

            // No alignment if we inject a checkpoint
            //如果我们注入检查点，则不对齐
            CheckpointMetricsBuilder checkpointMetrics =
                    new CheckpointMetricsBuilder()
                            .setAlignmentDurationNanos(0L)
                            .setBytesProcessedDuringAlignment(0L)
                            .setCheckpointStartDelayNanos(latestAsyncCheckpointStartDelayNanos);

            //初始化新检查点。
            subtaskCheckpointCoordinator.initInputsCheckpoint(
                    checkpointMetaData.getCheckpointId(), checkpointOptions);

            boolean success =
                    //执行检查点
                    performCheckpoint(checkpointMetaData, checkpointOptions, checkpointMetrics);
            if (!success) {
                //拒绝检查点
                declineCheckpoint(checkpointMetaData.getCheckpointId());
            }
            return success;
        } catch (Exception e) {
            // propagate exceptions only if the task is still in "running" state
            //仅当任务仍处于“运行”状态时传播异常
            if (isRunning) {
                throw new Exception(
                        "Could not perform checkpoint "
                                + checkpointMetaData.getCheckpointId()
                                + " for operator "
                                + getName()
                                + '.',
                        e);
            } else {
                LOG.debug(
                        "Could not perform checkpoint {} for operator {} while the "
                                + "invokable was not in state running.",
                        checkpointMetaData.getCheckpointId(),
                        getName(),
                        e);
                return false;
            }
        } finally {
            FlinkSecurityManager.unmonitorUserSystemExitForCurrentThread();
        }
    }

    private boolean triggerUnfinishedChannelsCheckpoint(
            CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions)
            throws Exception {
        Optional<CheckpointBarrierHandler> checkpointBarrierHandler = getCheckpointBarrierHandler();
        checkState(
                checkpointBarrierHandler.isPresent(),
                "CheckpointBarrier should exist for tasks with network inputs.");

        CheckpointBarrier barrier =
                new CheckpointBarrier(
                        checkpointMetaData.getCheckpointId(),
                        checkpointMetaData.getTimestamp(),
                        checkpointOptions);

        for (IndexedInputGate inputGate : getEnvironment().getAllInputGates()) {
            if (!inputGate.isFinished()) {
                for (InputChannelInfo channelInfo : inputGate.getUnfinishedChannels()) {
                    checkpointBarrierHandler.get().processBarrier(barrier, channelInfo, true);
                }
            }
        }

        return true;
    }

    /**
     * Acquires the optional {@link CheckpointBarrierHandler} associated with this stream task. The
     * {@code CheckpointBarrierHandler} should exist if the task has data inputs and requires to
     * align the barriers.
     */
    //获取与此流任务关联的可选CheckpointBarrierHandler 。
    // 如果任务有数据输入并且需要对齐障碍，则CheckpointBarrierHandler应该存在。
    protected Optional<CheckpointBarrierHandler> getCheckpointBarrierHandler() {
        return Optional.empty();
    }

    @Override
    public void triggerCheckpointOnBarrier(
            CheckpointMetaData checkpointMetaData,
            CheckpointOptions checkpointOptions,
            CheckpointMetricsBuilder checkpointMetrics)
            throws IOException {

        FlinkSecurityManager.monitorUserSystemExitForCurrentThread();
        try {
            //执行检查点
            performCheckpoint(checkpointMetaData, checkpointOptions, checkpointMetrics);
        } catch (CancelTaskException e) {
            LOG.info(
                    "Operator {} was cancelled while performing checkpoint {}.",
                    getName(),
                    checkpointMetaData.getCheckpointId());
            throw e;
        } catch (Exception e) {
            throw new IOException(
                    "Could not perform checkpoint "
                            + checkpointMetaData.getCheckpointId()
                            + " for operator "
                            + getName()
                            + '.',
                    e);
        } finally {
            FlinkSecurityManager.unmonitorUserSystemExitForCurrentThread();
        }
    }

    @Override
    public void abortCheckpointOnBarrier(long checkpointId, CheckpointException cause)
            throws IOException {
        if (isCurrentSyncSavepoint(checkpointId)) {
            throw new FlinkRuntimeException("Stop-with-savepoint failed.");
        }
        subtaskCheckpointCoordinator.abortCheckpointOnBarrier(checkpointId, cause, operatorChain);
    }

    private boolean performCheckpoint(
            CheckpointMetaData checkpointMetaData,
            CheckpointOptions checkpointOptions,
            CheckpointMetricsBuilder checkpointMetrics)
            throws Exception {

        final SnapshotType checkpointType = checkpointOptions.getCheckpointType();
        LOG.debug(
                "Starting checkpoint {} {} on task {}",
                checkpointMetaData.getCheckpointId(),
                checkpointType,
                getName());

        if (isRunning) {
            actionExecutor.runThrowing(
                    () -> {
                        //是同步的
                        if (isSynchronous(checkpointType)) {
                            setSynchronousSavepoint(checkpointMetaData.getCheckpointId());
                        }

                        //是否启用了已完成任务的检查点
                        if (areCheckpointsWithFinishedTasksEnabled()
                                && endOfDataReceived
                                && this.finalCheckpointMinId == null) {
                            this.finalCheckpointMinId = checkpointMetaData.getCheckpointId();
                        }

                        //检查点状态
                        subtaskCheckpointCoordinator.checkpointState(
                                checkpointMetaData,
                                checkpointOptions,
                                checkpointMetrics,
                                operatorChain,
                                finishedOperators,
                                this::isRunning);
                    });

            return true;
        } else {
            actionExecutor.runThrowing(
                    () -> {
                        // we cannot perform our checkpoint - let the downstream operators know that
                        // they
                        // should not wait for any input from this operator
                        //我们无法执行检查点 - 让下游操作员知道他们不应该等待来自该操作员的任何输入

                        // we cannot broadcast the cancellation markers on the 'operator chain',
                        // because it may not
                        // yet be created
                        //我们无法在“运营商链”上广播取消标记，因为它可能尚未创建
                        final CancelCheckpointMarker message =
                                new CancelCheckpointMarker(checkpointMetaData.getCheckpointId());
                        recordWriter.broadcastEvent(message);
                    });

            return false;
        }
    }

    private boolean isSynchronous(SnapshotType checkpointType) {
        return checkpointType.isSavepoint() && ((SavepointType) checkpointType).isSynchronous();
    }

    private void checkForcedFullSnapshotSupport(CheckpointOptions checkpointOptions) {
        if (checkpointOptions.getCheckpointType().equals(CheckpointType.FULL_CHECKPOINT)
                && !stateBackend.supportsNoClaimRestoreMode()) {
            throw new IllegalStateException(
                    String.format(
                            "Configured state backend (%s) does not support enforcing a full"
                                    + " snapshot. If you are restoring in %s mode, please"
                                    + " consider choosing %s mode.",
                            stateBackend, RestoreMode.NO_CLAIM, RestoreMode.CLAIM));
        } else if (checkpointOptions.getCheckpointType().isSavepoint()) {
            SavepointType savepointType = (SavepointType) checkpointOptions.getCheckpointType();
            if (!stateBackend.supportsSavepointFormat(savepointType.getFormatType())) {
                throw new IllegalStateException(
                        String.format(
                                "Configured state backend (%s) does not support %s savepoints",
                                stateBackend, savepointType.getFormatType()));
            }
        }
    }

    protected void declineCheckpoint(long checkpointId) {
        getEnvironment()
                .declineCheckpoint(
                        checkpointId,
                        new CheckpointException(
                                "Task Name" + getName(),
                                CheckpointFailureReason.CHECKPOINT_DECLINED_TASK_NOT_READY));
    }

    public final ExecutorService getAsyncOperationsThreadPool() {
        return asyncOperationsThreadPool;
    }

    @Override
    public Future<Void> notifyCheckpointCompleteAsync(long checkpointId) {
        return notifyCheckpointOperation(
                //通知检查点完成
                () -> notifyCheckpointComplete(checkpointId),
                String.format("checkpoint %d complete", checkpointId));
    }

    @Override
    public Future<Void> notifyCheckpointAbortAsync(
            long checkpointId, long latestCompletedCheckpointId) {
        return notifyCheckpointOperation(
                () -> {
                    if (latestCompletedCheckpointId > 0) {
                        notifyCheckpointComplete(latestCompletedCheckpointId);
                    }

                    if (isCurrentSyncSavepoint(checkpointId)) {
                        throw new FlinkRuntimeException("Stop-with-savepoint failed.");
                    }
                    subtaskCheckpointCoordinator.notifyCheckpointAborted(
                            checkpointId, operatorChain, this::isRunning);
                },
                String.format("checkpoint %d aborted", checkpointId));
    }

    @Override
    public Future<Void> notifyCheckpointSubsumedAsync(long checkpointId) {
        return notifyCheckpointOperation(
                () ->
                        subtaskCheckpointCoordinator.notifyCheckpointSubsumed(
                                checkpointId, operatorChain, this::isRunning),
                String.format("checkpoint %d subsumed", checkpointId));
    }

    private Future<Void> notifyCheckpointOperation(
            RunnableWithException runnable, String description) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        mailboxProcessor
                .getMailboxExecutor(TaskMailbox.MAX_PRIORITY)
                .execute(
                        () -> {
                            try {
                                runnable.run();
                            } catch (Exception ex) {
                                result.completeExceptionally(ex);
                                throw ex;
                            }
                            result.complete(null);
                        },
                        description);
        return result;
    }

    private void notifyCheckpointComplete(long checkpointId) throws Exception {
        LOG.debug("Notify checkpoint {} complete on task {}", checkpointId, getName());

        if (checkpointId <= latestReportCheckpointId) {
            return;
        }

        latestReportCheckpointId = checkpointId;

        //通知检查点完成
        subtaskCheckpointCoordinator.notifyCheckpointComplete(
                checkpointId, operatorChain, this::isRunning);
        if (isRunning) {
            if (isCurrentSyncSavepoint(checkpointId)) {
                finalCheckpointCompleted.complete(null);
            } else if (syncSavepoint == null
                    && finalCheckpointMinId != null
                    && checkpointId >= finalCheckpointMinId) {
                finalCheckpointCompleted.complete(null);
            }
        }
    }

    private void tryShutdownTimerService() {
        final long timeoutMs =
                getEnvironment()
                        .getTaskManagerInfo()
                        .getConfiguration()
                        .get(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT_TIMERS)
                        .toMillis();
        tryShutdownTimerService(timeoutMs, timerService);
        tryShutdownTimerService(timeoutMs, systemTimerService);
    }

    private void tryShutdownTimerService(long timeoutMs, TimerService timerService) {
        if (!timerService.isTerminated()) {
            if (!timerService.shutdownServiceUninterruptible(timeoutMs)) {
                LOG.warn(
                        "Timer service shutdown exceeded time limit of {} ms while waiting for pending "
                                + "timers. Will continue with shutdown procedure.",
                        timeoutMs);
            }
        }
    }

    // ------------------------------------------------------------------------
    //  Operator Events
    // ------------------------------------------------------------------------

    @Override
    public void dispatchOperatorEvent(OperatorID operator, SerializedValue<OperatorEvent> event)
            throws FlinkException {
        try {
            mainMailboxExecutor.execute(
                    () -> operatorChain.dispatchOperatorEvent(operator, event),
                    "dispatch operator event");
        } catch (RejectedExecutionException e) {
            // this happens during shutdown, we can swallow this
        }
    }

    // ------------------------------------------------------------------------
    //  State backend
    // ------------------------------------------------------------------------

    private StateBackend createStateBackend() throws Exception {
        //获取用户配置的StateBackend，配置项为:state.backend.type
        final StateBackend fromApplication =
                configuration.getStateBackend(getUserCodeClassLoader());

        //状态后端加载器
        return StateBackendLoader.fromApplicationOrConfigOrDefault(
                fromApplication,
                getJobConfiguration(),
                getEnvironment().getTaskManagerInfo().getConfiguration(),
                getUserCodeClassLoader(),
                LOG);
    }

    private CheckpointStorage createCheckpointStorage(StateBackend backend) throws Exception {
        final CheckpointStorage fromApplication =
                configuration.getCheckpointStorage(getUserCodeClassLoader());
        return CheckpointStorageLoader.load(
                fromApplication,
                backend,
                getJobConfiguration(),
                getEnvironment().getTaskManagerInfo().getConfiguration(),
                getUserCodeClassLoader(),
                LOG);
    }

    /**
     * Returns the {@link TimerService} responsible for telling the current processing time and
     * registering actual timers.
     */
    //返回负责告知当前处理时间并注册实际计时器的TimerService 。
    @VisibleForTesting
    TimerService getTimerService() {
        return timerService;
    }

    @VisibleForTesting
    OP getMainOperator() {
        return this.mainOperator;
    }

    @VisibleForTesting
    StreamTaskActionExecutor getActionExecutor() {
        return actionExecutor;
    }

    public ProcessingTimeServiceFactory getProcessingTimeServiceFactory() {
        return mailboxExecutor ->
                new ProcessingTimeServiceImpl(
                        timerService,
                        callback -> deferCallbackToMailbox(mailboxExecutor, callback));
    }

    /**
     * Handles an exception thrown by another thread (e.g. a TriggerTask), other than the one
     * executing the main task by failing the task entirely.
     *
     * <p>In more detail, it marks task execution failed for an external reason (a reason other than
     * the task code itself throwing an exception). If the task is already in a terminal state (such
     * as FINISHED, CANCELED, FAILED), or if the task is already canceling this does nothing.
     * Otherwise it sets the state to FAILED, and, if the invokable code is running, starts an
     * asynchronous thread that aborts that code.
     *
     * <p>This method never blocks.
     */
    //处理由另一个线程（例如 TriggerTask）抛出的异常，而不是通过完全失败来执行主任务的线程。
    //更详细地说，它标记任务执行因外部原因（任务代码本身引发异常之外的原因）失败。
    // 如果任务已处于最终状态（例如 FINISHED、CANCELED、FAILED），或者任务已在取消，则不会执行任何操作。
    // 否则，它将状态设置为 FAILED，并且如果可调用代码正在运行，则启动一个异步线程来中止该代码。
    //此方法永远不会阻塞。
    @Override
    public void handleAsyncException(String message, Throwable exception) {
        if (isRestoring || isRunning) {
            // only fail if the task is still in restoring or running
            //仅当任务仍在恢复或运行时才会失败
            asyncExceptionHandler.handleAsyncException(message, exception);
        }
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        return getName();
    }

    // ------------------------------------------------------------------------

    /** Utility class to encapsulate the handling of asynchronous exceptions. */
    //封装异步异常处理的实用程序类。
    static class StreamTaskAsyncExceptionHandler implements AsyncExceptionHandler {
        private final Environment environment;

        StreamTaskAsyncExceptionHandler(Environment environment) {
            this.environment = environment;
        }

        @Override
        public void handleAsyncException(String message, Throwable exception) {
            environment.failExternally(new AsynchronousException(message, exception));
        }
    }

    public final CloseableRegistry getCancelables() {
        return cancelables;
    }

    // ------------------------------------------------------------------------

    @VisibleForTesting
    public static <OUT>
            RecordWriterDelegate<SerializationDelegate<StreamRecord<OUT>>>
                    createRecordWriterDelegate(
                            StreamConfig configuration, Environment environment) {
        List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> recordWrites =
                createRecordWriters(configuration, environment);
        if (recordWrites.size() == 1) {
            return new SingleRecordWriter<>(recordWrites.get(0));
        } else if (recordWrites.size() == 0) {
            return new NonRecordWriter<>();
        } else {
            return new MultipleRecordWriters<>(recordWrites);
        }
    }

    private static <OUT>
            List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> createRecordWriters(
                    StreamConfig configuration, Environment environment) {
        List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> recordWriters =
                new ArrayList<>();
        //先获取每个算子不可chain下游的出边集合，
        List<NonChainedOutput> outputsInOrder =
                configuration.getVertexNonChainedOutputs(
                        environment.getUserCodeClassLoader().asClassLoader());

        int index = 0;
        //遍历该集合，根据每个不可chain下游算子的出边创建一个RecordWriter实例。
        for (NonChainedOutput streamOutput : outputsInOrder) {
            replaceForwardPartitionerIfConsumerParallelismDoesNotMatch(
                    environment, streamOutput, index);
            recordWriters.add(
                    //创建
                    createRecordWriter(
                            streamOutput,
                            index++,
                            environment,
                            environment.getTaskInfo().getTaskNameWithSubtasks(),
                            streamOutput.getBufferTimeout()));
        }
        return recordWriters;
    }

    private static void replaceForwardPartitionerIfConsumerParallelismDoesNotMatch(
            Environment environment, NonChainedOutput streamOutput, int outputIndex) {
        if (streamOutput.getPartitioner() instanceof ForwardPartitioner
                && environment.getWriter(outputIndex).getNumberOfSubpartitions()
                        != environment.getTaskInfo().getNumberOfParallelSubtasks()) {
            LOG.debug(
                    "Replacing forward partitioner with rebalance for {}",
                    environment.getTaskInfo().getTaskNameWithSubtasks());
            streamOutput.setPartitioner(new RebalancePartitioner<>());
        }
    }

    @SuppressWarnings("unchecked")
    private static <OUT> RecordWriter<SerializationDelegate<StreamRecord<OUT>>> createRecordWriter(
            NonChainedOutput streamOutput,
            int outputIndex,
            Environment environment,
            String taskNameWithSubtask,
            long bufferTimeout) {

        StreamPartitioner<OUT> outputPartitioner = null;

        // Clones the partition to avoid multiple stream edges sharing the same stream partitioner,
        // like the case of https://issues.apache.org/jira/browse/FLINK-14087.
        //克隆分区以避免多个流边缘共享相同的流分区器，例如 https:issues.apache.orgjirabrowseFLINK-14087 的情况。
        try {
            outputPartitioner =
                    InstantiationUtil.clone(
                            (StreamPartitioner<OUT>) streamOutput.getPartitioner(),
                            environment.getUserCodeClassLoader().asClassLoader());
        } catch (Exception e) {
            ExceptionUtils.rethrow(e);
        }

        LOG.debug(
                "Using partitioner {} for output {} of task {}",
                outputPartitioner,
                outputIndex,
                taskNameWithSubtask);

        //从ResultPartitionWriter[]中获取
        ResultPartitionWriter bufferWriter = environment.getWriter(outputIndex);

        // we initialize the partitioner here with the number of key groups (aka max. parallelism)
        //我们在这里用键组的数量 (也称为最大并行性) 初始化分区程序
        if (outputPartitioner instanceof ConfigurableStreamPartitioner) {
            int numKeyGroups = bufferWriter.getNumTargetKeyGroups();
            if (0 < numKeyGroups) {
                ((ConfigurableStreamPartitioner) outputPartitioner).configure(numKeyGroups);
            }
        }

        RecordWriter<SerializationDelegate<StreamRecord<OUT>>> output =
                new RecordWriterBuilder<SerializationDelegate<StreamRecord<OUT>>>()
                        .setChannelSelector(outputPartitioner)
                        .setTimeout(bufferTimeout)
                        .setTaskName(taskNameWithSubtask)
                        //构建RecordWriter
                        .build(bufferWriter);
        output.setMetricGroup(environment.getMetricGroup().getIOMetricGroup());
        return output;
    }

    private void handleTimerException(Exception ex) {
        handleAsyncException("Caught exception while processing timer.", new TimerException(ex));
    }

    @VisibleForTesting
    ProcessingTimeCallback deferCallbackToMailbox(
            MailboxExecutor mailboxExecutor, ProcessingTimeCallback callback) {
        return timestamp -> {
            mailboxExecutor.execute(
                    () -> invokeProcessingTimeCallback(callback, timestamp),
                    "Timer callback for %s @ %d",
                    callback,
                    timestamp);
        };
    }

    private void invokeProcessingTimeCallback(ProcessingTimeCallback callback, long timestamp) {
        try {
            callback.onProcessingTime(timestamp);
        } catch (Throwable t) {
            handleAsyncException("Caught exception while processing timer.", new TimerException(t));
        }
    }

    protected long getAsyncCheckpointStartDelayNanos() {
        return latestAsyncCheckpointStartDelayNanos;
    }

    private static class ResumeWrapper implements Runnable {
        private final Suspension suspendedDefaultAction;
        @Nullable private final PeriodTimer timer;

        public ResumeWrapper(Suspension suspendedDefaultAction, @Nullable PeriodTimer timer) {
            this.suspendedDefaultAction = suspendedDefaultAction;
            if (timer != null) {
                timer.markStart();
            }
            this.timer = timer;
        }

        @Override
        public void run() {
            if (timer != null) {
                timer.markEnd();
            }
            suspendedDefaultAction.resume();
        }
    }

    @Override
    public boolean isUsingNonBlockingInput() {
        return true;
    }

    /**
     * While we are outside the user code, we do not want to be interrupted further upon
     * cancellation. The shutdown logic below needs to make sure it does not issue calls that block
     * and stall shutdown. Additionally, the cancellation watch dog will issue a hard-cancel (kill
     * the TaskManager process) as a backup in case some shutdown procedure blocks outside our
     * control.
     */
    //虽然我们在用户代码之外，但我们不希望在取消后被进一步中断。下面的关闭逻辑需要确保它不会发出阻止和停止关闭的调用。
    // 此外，取消看门狗将发出硬取消（终止 TaskManager 进程）作为备份，以防某些关闭过程在我们的控制之外发生阻塞。
    private void disableInterruptOnCancel() {
        synchronized (shouldInterruptOnCancelLock) {
            shouldInterruptOnCancel = false;
        }
    }

    @Override
    public void maybeInterruptOnCancel(
            Thread toInterrupt, @Nullable String taskName, @Nullable Long timeout) {
        synchronized (shouldInterruptOnCancelLock) {
            if (shouldInterruptOnCancel) {
                if (taskName != null && timeout != null) {
                    Task.logTaskThreadStackTrace(toInterrupt, taskName, timeout, "interrupting");
                }

                toInterrupt.interrupt();
            }
        }
    }

    @Override
    public final Environment getEnvironment() {
        return environment;
    }

    /** Check whether records can be emitted in batch. */
    //检查是否可以批量发出记录。
    @FunctionalInterface
    public interface CanEmitBatchOfRecordsChecker {

        boolean check();
    }
}
