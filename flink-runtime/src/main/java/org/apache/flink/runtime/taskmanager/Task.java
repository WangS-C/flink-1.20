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

package org.apache.flink.runtime.taskmanager;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobInfo;
import org.apache.flink.api.common.JobInfoImpl;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.TaskInfoImpl;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.fs.FileSystemSafetyNet;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.security.FlinkSecurityManager;
import org.apache.flink.runtime.accumulators.AccumulatorRegistry;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointStoreUtil;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriteRequestExecutorFactory;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.externalresource.ExternalResourceInfoProvider;
import org.apache.flink.runtime.filecache.FileCache;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.PartitionProducerStateProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointableTask;
import org.apache.flink.runtime.jobgraph.tasks.CoordinatedTask;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.jobgraph.tasks.TaskInvokable;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.SharedResources;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.TaskNotRunningException;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.shuffle.ShuffleIOOwnerContext;
import org.apache.flink.runtime.state.TaskStateManager;
import org.apache.flink.runtime.taskexecutor.GlobalAggregateManager;
import org.apache.flink.runtime.taskexecutor.KvStateService;
import org.apache.flink.runtime.taskexecutor.PartitionProducerStateChecker;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlotPayload;
import org.apache.flink.types.Either;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FatalExitExceptionHandler;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.MdcUtils;
import org.apache.flink.util.MdcUtils.MdcCloseable;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TaskManagerExceptionUtils;
import org.apache.flink.util.UserCodeClassLoader;
import org.apache.flink.util.WrappingRuntimeException;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.RunnableWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The Task represents one execution of a parallel subtask on a TaskManager. A Task wraps a Flink
 * operator (which may be a user function) and runs it, providing all services necessary for example
 * to consume input data, produce its results (intermediate result partitions) and communicate with
 * the JobManager.
 *
 * <p>The Flink operators (implemented as subclasses of {@link TaskInvokable} have only data
 * readers, writers, and certain event callbacks. The task connects those to the network stack and
 * actor messages, and tracks the state of the execution and handles exceptions.
 *
 * <p>Tasks have no knowledge about how they relate to other tasks, or whether they are the first
 * attempt to execute the task, or a repeated attempt. All of that is only known to the JobManager.
 * All the task knows are its own runnable code, the task's configuration, and the IDs of the
 * intermediate results to consume and produce (if any).
 *
 * <p>Each Task is run by one dedicated thread.
 */
//该任务表示TaskManager上的并行子任务的一次执行。
//
//Task包装Flink操作符 (可能是用户函数) 并运行它，提供所有必要的服务，例如消费输入数据，产生其结果 (中间结果分区) 以及与JobManager通信。
//Flink operator (实现为TaskInvokable的子类，只有数据读取器、写入器和某些事件回调，任务将它们连接到网络堆栈和actor消息，并跟踪执行的状态和处理异常。
//任务不知道它们与其他任务的关系，也不知道它们是第一次尝试执行任务，还是重复尝试。所有这些只有JobManager知道。
//任务所知道的只是它自己的可运行代码、任务的配置以及要使用和产生的中间结果的id (如果有的话)。
//每个任务由一个专用线程运行。
public class Task
        implements Runnable, TaskSlotPayload, TaskActions, PartitionProducerStateProvider {

    /** The class logger. */
    private static final Logger LOG = LoggerFactory.getLogger(Task.class);

    /** The thread group that contains all task threads. */
    //包含所有任务线程的线程组。
    private static final ThreadGroup TASK_THREADS_GROUP = new ThreadGroup("Flink Task Threads");

    /** For atomic state updates. */
    //用于原子状态更新。
    private static final AtomicReferenceFieldUpdater<Task, ExecutionState> STATE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(
                    Task.class, ExecutionState.class, "executionState");

    // ------------------------------------------------------------------------
    //  Constant fields that are part of the initial Task construction
    // ------------------------------------------------------------------------

    /** The job that the task belongs to. */
    //任务所属的作业。
    private final JobID jobId;

    /** The type of this job. */
    //此作业的类型。
    private final JobType jobType;

    /** The vertex in the JobGraph whose code the task executes. */
    //任务执行其代码的JobGraph中的顶点。
    private final JobVertexID vertexId;

    /** The execution attempt of the parallel subtask. */
    //并行子任务的执行尝试。
    private final ExecutionAttemptID executionId;

    /** ID which identifies the slot in which the task is supposed to run. */
    //标识应该在其中运行任务的槽的ID。
    private final AllocationID allocationId;

    /** The meta information of current job. */
    //当前作业的 Job 信息。
    private final JobInfo jobInfo;

    /** The meta information of current task. */
    //当前任务的Task信息。
    private final TaskInfo taskInfo;

    /** The name of the task, including subtask indexes. */
    //任务的名称，包括子任务索引。
    private final String taskNameWithSubtask;

    /** The job-wide configuration object. */
    //作业范围的配置对象
    private final Configuration jobConfiguration;

    /** The task-specific configuration. */
    //特定于任务的配置。
    private final Configuration taskConfiguration;

    /** The jar files used by this task. */
    //此任务使用的jar文件。
    private final Collection<PermanentBlobKey> requiredJarFiles;

    /** The classpaths used by this task. */
    //此任务使用的类路径。
    private final Collection<URL> requiredClasspaths;

    /** The name of the class that holds the invokable code. */
    //包含invokable代码的类的名称。
    private final String nameOfInvokableClass;

    /** Access to task manager configuration and host names. */
    //访问任务管理器配置和主机名。
    private final TaskManagerRuntimeInfo taskManagerConfig;

    /** The memory manager to be used by this task. */
    //此任务要使用的内存管理器。
    private final MemoryManager memoryManager;

    /** Shared memory manager provided by the task manager. */
    //任务管理器提供的共享内存管理器。
    private final SharedResources sharedResources;

    /** The I/O manager to be used by this task. */
    //此任务要使用的I/O管理器。
    private final IOManager ioManager;

    /** The BroadcastVariableManager to be used by this task. */
    //此任务要使用的BroadcastVariableManager。
    private final BroadcastVariableManager broadcastVariableManager;

    private final TaskEventDispatcher taskEventDispatcher;

    /** Information provider for external resources. */
    //外部资源的信息提供者
    private final ExternalResourceInfoProvider externalResourceInfoProvider;

    /** The manager for state of operators running in this task/slot. */
    //此任务/ 插槽中运行的操作员状态的管理器。
    private final TaskStateManager taskStateManager;

    /**
     * Serialized version of the job specific execution configuration (see {@link ExecutionConfig}).
     */
    //作业特定执行配置的序列化版本 (请参阅ExecutionConfig )。
    private final SerializedValue<ExecutionConfig> serializedExecutionConfig;

    //ResultPartitionWriter面向的是Buffer，在数据传输层次中处于最低层，
    //其子类实现中包含一个BufferPool组件，提供Buffer资源。
    //子类实现中包含一个数组结构ResultSubpartition[] subpartitions的子分区组件，
    //用来承接上层RecordWriter对象分发下来的数据。
    private final ResultPartitionWriter[] partitionWriters;

    private final IndexedInputGate[] inputGates;

    /** Connection to the task manager. */
    //连接到任务管理器。
    private final TaskManagerActions taskManagerActions;

    /** Input split provider for the task. */
    //输入任务的拆分提供程序。
    private final InputSplitProvider inputSplitProvider;

    /** Checkpoint notifier used to communicate with the CheckpointCoordinator. */
    //用于与CheckpointCoordinator通信的检查点通知程序。
    private final CheckpointResponder checkpointResponder;

    /**
     * The gateway for operators to send messages to the operator coordinators on the Job Manager.
     */
    //操作员向作业管理器上的操作员协调员发送消息的网关。
    private final TaskOperatorEventGateway operatorCoordinatorEventGateway;

    /** GlobalAggregateManager used to update aggregates on the JobMaster. */
    //GlobalAggregateManager用于更新JobMaster上的聚合。
    private final GlobalAggregateManager aggregateManager;

    /** The library cache, from which the task can request its class loader. */
    //库缓存，任务可以从中请求其类加载器。
    private final LibraryCacheManager.ClassLoaderHandle classLoaderHandle;

    /** The cache for user-defined files that the invokable requires. */
    //invokable所需的用户定义文件的缓存。
    private final FileCache fileCache;

    /** The service for kvState registration of this task. */
    //此任务的kvState注册服务。
    private final KvStateService kvStateService;

    /** The registry of this task which enables live reporting of accumulators. */
    //此任务的注册表，它启用累加器的实时报告。
    private final AccumulatorRegistry accumulatorRegistry;

    /** The thread that executes the task. */
    //执行任务的线程。
    private final Thread executingThread;

    /** Parent group for all metrics of this task. */
    private final TaskMetricGroup metrics;

    /** Partition producer state checker to request partition states from. */
    //请求分区状态的分区生产者状态检查器。
    private final PartitionProducerStateChecker partitionProducerStateChecker;

    /** Executor to run future callbacks. */
    private final Executor executor;

    /** Future that is completed once {@link #run()} exits. */
    private final CompletableFuture<ExecutionState> terminationFuture = new CompletableFuture<>();

    /** The factory of channel state write request executor. */
    private final ChannelStateWriteRequestExecutorFactory channelStateExecutorFactory;

    // ------------------------------------------------------------------------
    //  Fields that control the task execution. All these fields are volatile
    //  (which means that they introduce memory barriers), to establish
    //  proper happens-before semantics on parallel modification
    // ------------------------------------------------------------------------

    /** atomic flag that makes sure the invokable is canceled exactly once upon error. */
    //原子标志，确保invokable在出错时被完全取消。
    private final AtomicBoolean invokableHasBeenCanceled;
    /**
     * The invokable of this task, if initialized. All accesses must copy the reference and check
     * for null, as this field is cleared as part of the disposal logic.
     */
    //此任务的invokable (如果已初始化)。所有访问都必须复制引用并检查null，因为此字段作为处置逻辑的一部分被清除。
    @Nullable private volatile TaskInvokable invokable;

    /** The current execution state of the task. */
    //任务的当前执行状态。
    private volatile ExecutionState executionState = ExecutionState.CREATED;

    /** The observed exception, in case the task execution failed. */
    //观察到的异常，以防任务执行失败。
    private volatile Throwable failureCause;

    /** Initialized from the Flink configuration. May also be set at the ExecutionConfig */
    //从Flink配置初始化。也可以在ExecutionConfig中设置
    private long taskCancellationInterval;

    /** Initialized from the Flink configuration. May also be set at the ExecutionConfig */
    private long taskCancellationTimeout;

    /**
     * This class loader should be set as the context class loader for threads that may dynamically
     * load user code.
     */
    //对于可以动态加载用户代码的线程，应该将该类加载器设置为上下文类加载器。
    private UserCodeClassLoader userCodeClassLoader;

    /**
     * <b>IMPORTANT:</b> This constructor may not start any work that would need to be undone in the
     * case of a failing task deployment.
     */
    //重要提示: 在任务部署失败的情况下，此构造函数可能不会启动任何需要撤消的工作。
    public Task(
            JobInformation jobInformation,
            TaskInformation taskInformation,
            ExecutionAttemptID executionAttemptID,
            AllocationID slotAllocationId,
            List<ResultPartitionDeploymentDescriptor> resultPartitionDeploymentDescriptors,
            List<InputGateDeploymentDescriptor> inputGateDeploymentDescriptors,
            MemoryManager memManager,
            SharedResources sharedResources,
            IOManager ioManager,
            ShuffleEnvironment<?, ?> shuffleEnvironment,
            KvStateService kvStateService,
            BroadcastVariableManager bcVarManager,
            TaskEventDispatcher taskEventDispatcher,
            ExternalResourceInfoProvider externalResourceInfoProvider,
            TaskStateManager taskStateManager,
            TaskManagerActions taskManagerActions,
            InputSplitProvider inputSplitProvider,
            CheckpointResponder checkpointResponder,
            TaskOperatorEventGateway operatorCoordinatorEventGateway,
            GlobalAggregateManager aggregateManager,
            LibraryCacheManager.ClassLoaderHandle classLoaderHandle,
            FileCache fileCache,
            TaskManagerRuntimeInfo taskManagerConfig,
            @Nonnull TaskMetricGroup metricGroup,
            PartitionProducerStateChecker partitionProducerStateChecker,
            Executor executor,
            ChannelStateWriteRequestExecutorFactory channelStateExecutorFactory) {

        Preconditions.checkNotNull(jobInformation);
        Preconditions.checkNotNull(taskInformation);
        this.jobInfo = new JobInfoImpl(jobInformation.getJobId(), jobInformation.getJobName());
        this.taskInfo =
                new TaskInfoImpl(
                        taskInformation.getTaskName(),
                        taskInformation.getMaxNumberOfSubtasks(),
                        executionAttemptID.getSubtaskIndex(),
                        taskInformation.getNumberOfSubtasks(),
                        executionAttemptID.getAttemptNumber(),
                        String.valueOf(slotAllocationId));

        this.jobId = jobInformation.getJobId();
        this.jobType = jobInformation.getJobType();
        this.vertexId = taskInformation.getJobVertexId();
        this.executionId = Preconditions.checkNotNull(executionAttemptID);
        this.allocationId = Preconditions.checkNotNull(slotAllocationId);
        this.taskNameWithSubtask = taskInfo.getTaskNameWithSubtasks();
        this.jobConfiguration = jobInformation.getJobConfiguration();
        this.taskConfiguration = taskInformation.getTaskConfiguration();
        this.requiredJarFiles = jobInformation.getRequiredJarFileBlobKeys();
        this.requiredClasspaths = jobInformation.getRequiredClasspathURLs();
        this.nameOfInvokableClass = taskInformation.getInvokableClassName();
        this.serializedExecutionConfig = jobInformation.getSerializedExecutionConfig();

        Configuration tmConfig = taskManagerConfig.getConfiguration();
        this.taskCancellationInterval =
                tmConfig.get(TaskManagerOptions.TASK_CANCELLATION_INTERVAL).toMillis();
        this.taskCancellationTimeout =
                tmConfig.get(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT).toMillis();

        this.memoryManager = Preconditions.checkNotNull(memManager);
        this.sharedResources = Preconditions.checkNotNull(sharedResources);
        this.ioManager = Preconditions.checkNotNull(ioManager);
        this.broadcastVariableManager = Preconditions.checkNotNull(bcVarManager);
        this.taskEventDispatcher = Preconditions.checkNotNull(taskEventDispatcher);
        this.taskStateManager = Preconditions.checkNotNull(taskStateManager);
        this.accumulatorRegistry = new AccumulatorRegistry(jobId, executionId);

        this.inputSplitProvider = Preconditions.checkNotNull(inputSplitProvider);
        this.checkpointResponder = Preconditions.checkNotNull(checkpointResponder);
        this.operatorCoordinatorEventGateway =
                Preconditions.checkNotNull(operatorCoordinatorEventGateway);
        this.aggregateManager = Preconditions.checkNotNull(aggregateManager);
        this.taskManagerActions = checkNotNull(taskManagerActions);
        this.externalResourceInfoProvider = checkNotNull(externalResourceInfoProvider);

        this.classLoaderHandle = Preconditions.checkNotNull(classLoaderHandle);
        this.fileCache = Preconditions.checkNotNull(fileCache);
        this.kvStateService = Preconditions.checkNotNull(kvStateService);
        this.taskManagerConfig = Preconditions.checkNotNull(taskManagerConfig);

        this.metrics = metricGroup;

        this.partitionProducerStateChecker =
                Preconditions.checkNotNull(partitionProducerStateChecker);
        this.executor = Preconditions.checkNotNull(executor);
        this.channelStateExecutorFactory = channelStateExecutorFactory;

        // create the reader and writer structures
        //创建reader和writer结构
        final String taskNameWithSubtaskAndId = taskNameWithSubtask + " (" + executionId + ')';

        final ShuffleIOOwnerContext taskShuffleContext =
                //创建 shuffle IO 所有者上下文
                shuffleEnvironment.createShuffleIOOwnerContext(
                        taskNameWithSubtaskAndId, executionId, metrics.getIOMetricGroup());

        // produced intermediate result partitions
        //Task的输出操作
        final ResultPartitionWriter[] resultPartitionWriters =
                shuffleEnvironment
                        //负责创建partitionWriters成员变量，
                        //由入参可知包含Task提交过程中创建的ResultPartitionDeploymentDescriptor实例信息，即Task的输出信息。
                        .createResultPartitionWriters(
                                taskShuffleContext, resultPartitionDeploymentDescriptors)
                        .toArray(new ResultPartitionWriter[] {});

        this.partitionWriters = resultPartitionWriters;

        // consumed intermediate result partitions
        //消耗的中间结果分区
        final IndexedInputGate[] gates =
                shuffleEnvironment
                        //inputGates的生成入口，入参包含Task提交过程中创建的InputGateDeploymentDescriptor实例信息即Task的输入信
                        .createInputGates(taskShuffleContext, this, inputGateDeploymentDescriptors)
                        .toArray(new IndexedInputGate[0]);

        //Task的输入操作
        this.inputGates = new IndexedInputGate[gates.length];
        int counter = 0;
        for (IndexedInputGate gate : gates) {
            inputGates[counter++] =
                    new InputGateWithMetrics(
                            gate, metrics.getIOMetricGroup().getNumBytesInCounter());
        }

        if (shuffleEnvironment instanceof NettyShuffleEnvironment) {
            //noinspection deprecation
            ((NettyShuffleEnvironment) shuffleEnvironment)
                    .registerLegacyNetworkMetrics(
                            metrics.getIOMetricGroup(), resultPartitionWriters, gates);
        }

        invokableHasBeenCanceled = new AtomicBoolean(false);

        // finally, create the executing thread, but do not start it
        //最后，创建执行线程，但不启动它
        executingThread = new Thread(TASK_THREADS_GROUP, this, taskNameWithSubtask);
    }

    // ------------------------------------------------------------------------
    //  Accessors
    // ------------------------------------------------------------------------

    @Override
    public JobID getJobID() {
        return jobId;
    }

    public JobVertexID getJobVertexId() {
        return vertexId;
    }

    @Override
    public ExecutionAttemptID getExecutionId() {
        return executionId;
    }

    @Override
    public AllocationID getAllocationId() {
        return allocationId;
    }

    public TaskInfo getTaskInfo() {
        return taskInfo;
    }

    public Configuration getJobConfiguration() {
        return jobConfiguration;
    }

    public Configuration getTaskConfiguration() {
        return this.taskConfiguration;
    }

    public AccumulatorRegistry getAccumulatorRegistry() {
        return accumulatorRegistry;
    }

    public TaskMetricGroup getMetricGroup() {
        return metrics;
    }

    public Thread getExecutingThread() {
        return executingThread;
    }

    @Override
    public CompletableFuture<ExecutionState> getTerminationFuture() {
        return terminationFuture;
    }

    @VisibleForTesting
    long getTaskCancellationInterval() {
        return taskCancellationInterval;
    }

    @VisibleForTesting
    long getTaskCancellationTimeout() {
        return taskCancellationTimeout;
    }

    @Nullable
    @VisibleForTesting
    TaskInvokable getInvokable() {
        return invokable;
    }

    public boolean isBackPressured() {
        if (invokable == null
                || partitionWriters.length == 0
                || (executionState != ExecutionState.INITIALIZING
                        && executionState != ExecutionState.RUNNING)) {
            return false;
        }
        for (int i = 0; i < partitionWriters.length; ++i) {
            if (!partitionWriters[i].isAvailable()) {
                return true;
            }
        }
        return false;
    }

    // ------------------------------------------------------------------------
    //  Task Execution
    // ------------------------------------------------------------------------

    /**
     * Returns the current execution state of the task.
     *
     * @return The current execution state of the task.
     */
    //返回任务的当前执行状态
    public ExecutionState getExecutionState() {
        return this.executionState;
    }

    /**
     * Checks whether the task has failed, is canceled, or is being canceled at the moment.
     *
     * @return True is the task in state FAILED, CANCELING, or CANCELED, false otherwise.
     */
    //检查任务是否已失败、已取消或正在取消。
    //返回：
    //True 表示任务处于 FAILED、CANCELING 或 CANCELED 状态，否则为 false。
    public boolean isCanceledOrFailed() {
        return executionState == ExecutionState.CANCELING
                || executionState == ExecutionState.CANCELED
                || executionState == ExecutionState.FAILED;
    }

    /**
     * If the task has failed, this method gets the exception that caused this task to fail.
     * Otherwise this method returns null.
     *
     * @return The exception that caused the task to fail, or null, if the task has not failed.
     */
    //如果任务失败，此方法将获取导致该任务失败的异常。否则该方法返回 null。
    //返回：
    //导致任务失败的异常，如果任务未失败，则为 null。
    public Throwable getFailureCause() {
        return failureCause;
    }

    /** Starts the task's thread. */
   //启动任务的线程。
    public void startTaskThread() {
        executingThread.start();
    }

    /** The core work method that bootstraps the task and executes its code. */
    //引导任务并执行其代码的核心工作方法。
    @Override
    public void run() {
        try (MdcCloseable ignored = MdcUtils.withContext(MdcUtils.asContextData(jobId))) {
            doRun();
        } finally {
            terminationFuture.complete(executionState);
        }
    }

    private void doRun() {
        // ----------------------------
        //  Initial State transition
        // ----------------------------
        //初始状态转换
        while (true) {
            ExecutionState current = this.executionState;
            if (current == ExecutionState.CREATED) {
                if (transitionState(ExecutionState.CREATED, ExecutionState.DEPLOYING)) {
                    // success, we can start our work
                    //成功了，我们就可以开始我们的工作了
                    break;
                }
            } else if (current == ExecutionState.FAILED) {
                // we were immediately failed. tell the TaskManager that we reached our final state
                //我们立即失败了。告诉任务管理器我们到达了最终状态
                notifyFinalState();
                if (metrics != null) {
                    metrics.close();
                }
                return;
            } else if (current == ExecutionState.CANCELING) {
                if (transitionState(ExecutionState.CANCELING, ExecutionState.CANCELED)) {
                    // we were immediately canceled. tell the TaskManager that we reached our final
                    // state
                    //我们立即被取消。告诉任务管理器我们到达了最终状态
                    notifyFinalState();
                    if (metrics != null) {
                        metrics.close();
                    }
                    return;
                }
            } else {
                if (metrics != null) {
                    metrics.close();
                }
                throw new IllegalStateException(
                        "Invalid state for beginning of operation of task " + this + '.');
            }
        }

        // all resource acquisitions and registrations from here on
        // need to be undone in the end
        //从这里开始的所有资源获取和注册最终都需要撤消
        Map<String, Future<Path>> distributedCacheEntries = new HashMap<>();
        TaskInvokable invokable = null;

        try {
            // ----------------------------
            //  Task Bootstrap - We periodically
            //  check for canceling as a shortcut
            // ----------------------------
            //任务引导 - 我们定期检查取消作为快捷方式

            // activate safety net for task thread
            //激活任务线程的安全网
            LOG.debug("Creating FileSystem stream leak safety net for task {}", this);
            FileSystemSafetyNet.initializeSafetyNetForThread();

            // first of all, get a user-code classloader
            // this may involve downloading the job's JAR files and/or classes
            //首先，获取用户代码类加载器，这可能涉及下载作业的 JAR 文件和/或类
            LOG.info("Loading JAR files for task {}.", this);

            //创建用户代码类加载器
            userCodeClassLoader = createUserCodeClassloader();
            final ExecutionConfig executionConfig =
                    serializedExecutionConfig.deserializeValue(userCodeClassLoader.asClassLoader());
            Configuration executionConfigConfiguration = executionConfig.toConfiguration();

            // override task cancellation interval from Flink config if set in ExecutionConfig
            //如果在 ExecutionConfig 中设置，则覆盖 Flink 配置中的任务取消间隔
            taskCancellationInterval =
                    executionConfigConfiguration
                            .getOptional(TaskManagerOptions.TASK_CANCELLATION_INTERVAL)
                            .orElse(Duration.ofMillis(taskCancellationInterval))
                            .toMillis();

            // override task cancellation timeout from Flink config if set in ExecutionConfig
            //如果在 ExecutionConfig 中设置，则覆盖 Flink 配置中的任务取消超时
            taskCancellationTimeout =
                    executionConfigConfiguration
                            .getOptional(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT)
                            .orElse(Duration.ofMillis(taskCancellationTimeout))
                            .toMillis();

            //检查任务是否已失败、已取消或正在取消。
            if (isCanceledOrFailed()) {
                throw new CancelTaskException();
            }

            // ----------------------------------------------------------------
            // register the task with the network stack
            // this operation may fail if the system does not have enough
            // memory to run the necessary data exchanges
            // the registration must also strictly be undone
            // ----------------------------------------------------------------
            //向网络堆栈注册任务 如果系统没有足够的内存来运行必要的数据交换，则此操作可能会失败 还必须严格撤消注册

            LOG.debug("Registering task at network: {}.", this);

            //主要作用是设置ResultPartition和InputGate的BufferPool信息。
            setupPartitionsAndGates(partitionWriters, inputGates);

            for (ResultPartitionWriter partitionWriter : partitionWriters) {
                taskEventDispatcher.registerPartition(partitionWriter.getPartitionId());
            }

            // next, kick off the background copying of files for the distributed cache
            //接下来，启动分布式缓存的后台文件复制
            try {
                for (Map.Entry<String, DistributedCache.DistributedCacheEntry> entry :
                        DistributedCache.readFileInfoFromConfig(jobConfiguration)) {
                    LOG.info("Obtaining local cache file for '{}'.", entry.getKey());
                    Future<Path> cp =
                            fileCache.createTmpFile(
                                    entry.getKey(), entry.getValue(), jobId, executionId);
                    distributedCacheEntries.put(entry.getKey(), cp);
                }
            } catch (Exception e) {
                throw new Exception(
                        String.format(
                                "Exception while adding files to distributed cache of task %s (%s).",
                                taskNameWithSubtask, executionId),
                        e);
            }

            //检查任务是否已失败、已取消或正在取消。
            if (isCanceledOrFailed()) {
                throw new CancelTaskException();
            }

            // ----------------------------------------------------------------
            //  call the user code initialization methods
            // ----------------------------------------------------------------
            //调用用户代码初始化方法

            TaskKvStateRegistry kvStateRegistry =
                    kvStateService.createKvStateTaskRegistry(jobId, getJobVertexId());

            Environment env =
                    new RuntimeEnvironment(
                            jobId,
                            jobType,
                            vertexId,
                            executionId,
                            executionConfig,
                            jobInfo,
                            taskInfo,
                            jobConfiguration,
                            taskConfiguration,
                            userCodeClassLoader,
                            memoryManager,
                            sharedResources,
                            ioManager,
                            broadcastVariableManager,
                            taskStateManager,
                            aggregateManager,
                            accumulatorRegistry,
                            kvStateRegistry,
                            inputSplitProvider,
                            distributedCacheEntries,
                            partitionWriters,
                            inputGates,
                            taskEventDispatcher,
                            checkpointResponder,
                            operatorCoordinatorEventGateway,
                            taskManagerConfig,
                            metrics,
                            this,
                            externalResourceInfoProvider,
                            channelStateExecutorFactory,
                            taskManagerActions);

            // Make sure the user code classloader is accessible thread-locally.
            // We are setting the correct context class loader before instantiating the invokable
            // so that it is available to the invokable during its entire lifetime.
            //确保用户代码类加载器可本地线程访问。
            //我们在实例化可调用对象之前设置正确的上下文类加载器，以便可调用对象在其整个生命周期内都可以使用它。
            executingThread.setContextClassLoader(userCodeClassLoader.asClassLoader());

            // When constructing invokable, separate threads can be constructed and thus should be
            // monitored for system exit (in addition to invoking thread itself monitored below).
            //构造可调用时，可以构造单独的线程，因此应该监视系统退出（除了下面监视的调用线程本身之外）。
            FlinkSecurityManager.monitorUserSystemExitForCurrentThread();
            try {
                // now load and instantiate the task's invokable code
                //现在加载并实例化任务的可调用代码
                invokable =
                        //此处反射创建的invokable实例就是StreamTask实例。
                        loadAndInstantiateInvokable(
                                userCodeClassLoader.asClassLoader(), nameOfInvokableClass, env);
            } finally {
                FlinkSecurityManager.unmonitorUserSystemExitForCurrentThread();
            }

            // ----------------------------------------------------------------
            //  actual task core work
            // ----------------------------------------------------------------
            //实际任务核心工作

            // we must make strictly sure that the invokable is accessible to the cancel() call
            // by the time we switched to running.
            //我们必须严格确保当我们切换到运行状态时，cancel() 调用可以访问该可调用对象。
            this.invokable = invokable;

            //恢复和调用
            restoreAndInvoke(invokable);

            // make sure, we enter the catch block if the task leaves the invoke() method due
            // to the fact that it has been canceled
            //确保，如果任务由于已被取消而离开 invoke() 方法，我们将进入 catch 块
            if (isCanceledOrFailed()) {
                throw new CancelTaskException();
            }

            // ----------------------------------------------------------------
            //  finalization of a successful execution
            // ----------------------------------------------------------------
            //最终确定成功执行

            // finish the produced partitions. if this fails, we consider the execution failed.
            //完成制作的分区。如果失败，我们认为执行失败。
            for (ResultPartitionWriter partitionWriter : partitionWriters) {
                if (partitionWriter != null) {
                    partitionWriter.finish();
                }
            }

            // try to mark the task as finished
            // if that fails, the task was canceled/failed in the meantime
            //尝试将任务标记为已完成，如果失败，则任务已取消，同时失败
            if (!transitionState(ExecutionState.RUNNING, ExecutionState.FINISHED)) {
                throw new CancelTaskException();
            }
        } catch (Throwable t) {
            // ----------------------------------------------------------------
            // the execution failed. either the invokable code properly failed, or
            // an exception was thrown as a side effect of cancelling
            // ----------------------------------------------------------------
            //执行失败。可调用代码正确失败，或者作为取消的副作用而引发异常

            t = preProcessException(t);

            try {
                // transition into our final state. we should be either in DEPLOYING, INITIALIZING,
                // RUNNING, CANCELING, or FAILED
                // loop for multiple retries during concurrent state changes via calls to cancel()
                // or to failExternally()
                //过渡到我们的最终状态。
                //在并发状态更改期间，通过调用cancel()或failExternally()，
                //我们应该处于DEPLOYING、INITIALIZING、RUNNING、CANCELING或FAILED循环中进行多次重试
                while (true) {
                    ExecutionState current = this.executionState;

                    if (current == ExecutionState.RUNNING
                            || current == ExecutionState.INITIALIZING
                            || current == ExecutionState.DEPLOYING) {
                        if (ExceptionUtils.findThrowable(t, CancelTaskException.class)
                                .isPresent()) {
                            if (transitionState(current, ExecutionState.CANCELED, t)) {
                                cancelInvokable(invokable);
                                break;
                            }
                        } else {
                            if (transitionState(current, ExecutionState.FAILED, t)) {
                                cancelInvokable(invokable);
                                break;
                            }
                        }
                    } else if (current == ExecutionState.CANCELING) {
                        if (transitionState(current, ExecutionState.CANCELED)) {
                            break;
                        }
                    } else if (current == ExecutionState.FAILED) {
                        // in state failed already, no transition necessary any more
                        //状态已经失败，不再需要转换
                        break;
                    }
                    // unexpected state, go to failed
                    //意外状态，进入失败状态
                    else if (transitionState(current, ExecutionState.FAILED, t)) {
                        LOG.error(
                                "Unexpected state in task {} ({}) during an exception: {}.",
                                taskNameWithSubtask,
                                executionId,
                                current);
                        break;
                    }
                    // else fall through the loop and
                    //否则会掉入循环中
                }
            } catch (Throwable tt) {
                String message =
                        String.format(
                                "FATAL - exception in exception handler of task %s (%s).",
                                taskNameWithSubtask, executionId);
                LOG.error(message, tt);
                notifyFatalError(message, tt);
            }
        } finally {
            try {
                LOG.info("Freeing task resources for {} ({}).", taskNameWithSubtask, executionId);

                // clear the reference to the invokable. this helps guard against holding references
                // to the invokable and its structures in cases where this Task object is still
                // referenced
                //清除对可调用项的引用。这有助于防止在仍然引用此任务对象的情况下保留对可调用及其结构的引用
                this.invokable = null;

                // free the network resources
                //释放网络资源
                releaseResources();

                // free memory resources
                //释放内存资源
                if (invokable != null) {
                    memoryManager.releaseAll(invokable);
                }

                // remove all of the tasks resources
                //删除所有任务资源
                fileCache.releaseJob(jobId, executionId);

                // close and de-activate safety net for task thread
                //关闭并停用任务线程的安全网
                LOG.debug("Ensuring all FileSystem streams are closed for task {}", this);
                FileSystemSafetyNet.closeSafetyNetAndGuardedResourcesForThread();

                notifyFinalState();
            } catch (Throwable t) {
                // an error in the resource cleanup is fatal
                //资源清理中的错误是致命的
                String message =
                        String.format(
                                "FATAL - exception in resource cleanup of task %s (%s).",
                                taskNameWithSubtask, executionId);
                LOG.error(message, t);
                notifyFatalError(message, t);
            }

            // un-register the metrics at the end so that the task may already be
            // counted as finished when this happens
            // errors here will only be logged
            //最后取消注册指标，以便在发生这种情况时任务可能已经被计为完成，此处的错误只会被记录
            try {
                metrics.close();
            } catch (Throwable t) {
                LOG.error(
                        "Error during metrics de-registration of task {} ({}).",
                        taskNameWithSubtask,
                        executionId,
                        t);
            }
        }
    }

    /** Unwrap, enrich and handle fatal errors. */
    //展开、丰富并处理致命错误。
    private Throwable preProcessException(Throwable t) {
        // unwrap wrapped exceptions to make stack traces more compact
        //解开包装的异常以使堆栈跟踪更加紧凑
        if (t instanceof WrappingRuntimeException) {
            t = ((WrappingRuntimeException) t).unwrap();
        }

        TaskManagerExceptionUtils.tryEnrichTaskManagerError(t);

        // check if the exception is unrecoverable
        //检查异常是否不可恢复
        if (ExceptionUtils.isJvmFatalError(t)
                || (t instanceof OutOfMemoryError
                        && taskManagerConfig.shouldExitJvmOnOutOfMemoryError())) {

            // terminate the JVM immediately
            // don't attempt a clean shutdown, because we cannot expect the clean shutdown
            // to complete
            //立即终止 JVM 不要尝试干净关闭，因为我们不能期望干净关闭完成
            try {
                LOG.error(
                        "Encountered fatal error {} - terminating the JVM",
                        t.getClass().getName(),
                        t);
            } finally {
                Runtime.getRuntime().halt(-1);
            }
        }

        return t;
    }

    private void restoreAndInvoke(TaskInvokable finalInvokable) throws Exception {
        try {
            // switch to the INITIALIZING state, if that fails, we have been canceled/failed in the
            // meantime
            //切换到初始化状态，如果失败，我们已被取消失败在此期间
            if (!transitionState(ExecutionState.DEPLOYING, ExecutionState.INITIALIZING)) {
                throw new CancelTaskException();
            }

            taskManagerActions.updateTaskExecutionState(
                    new TaskExecutionState(executionId, ExecutionState.INITIALIZING));

            // make sure the user code classloader is accessible thread-locally
            //确保用户代码classloader是线程本地可访问的
            executingThread.setContextClassLoader(userCodeClassLoader.asClassLoader());

            //调用restore
            runWithSystemExitMonitoring(finalInvokable::restore);

            if (!transitionState(ExecutionState.INITIALIZING, ExecutionState.RUNNING)) {
                throw new CancelTaskException();
            }

            // notify everyone that we switched to running
            //通知大家我们已切换到RUNNING
            taskManagerActions.updateTaskExecutionState(
                    new TaskExecutionState(executionId, ExecutionState.RUNNING));

            //调用invoke
            runWithSystemExitMonitoring(finalInvokable::invoke);
        } catch (Throwable throwable) {
            try {
                runWithSystemExitMonitoring(() -> finalInvokable.cleanUp(throwable));
            } catch (Throwable cleanUpThrowable) {
                throwable.addSuppressed(cleanUpThrowable);
            }
            throw throwable;
        }
        runWithSystemExitMonitoring(() -> finalInvokable.cleanUp(null));
    }

    /**
     * Monitor user codes from exiting JVM covering user function invocation. This can be done in a
     * finer-grained way like enclosing user callback functions individually, but as exit triggered
     * by framework is not performed and expected in this invoke function anyhow, we can monitor
     * exiting JVM for entire scope.
     */
   //监视退出 JVM 中的用户代码（涵盖用户函数调用）。
    //这可以通过更细粒度的方式来完成，例如单独封装用户回调函数，
    //但由于框架触发的退出在该调用函数中并未执行和预期，因此我们可以在整个范围内监视退出 JVM。
    private void runWithSystemExitMonitoring(RunnableWithException action) throws Exception {
        FlinkSecurityManager.monitorUserSystemExitForCurrentThread();
        try {
            action.run();
        } finally {
            FlinkSecurityManager.unmonitorUserSystemExitForCurrentThread();
        }
    }

    @VisibleForTesting
    public static void setupPartitionsAndGates(
            ResultPartitionWriter[] producedPartitions, InputGate[] inputGates) throws IOException {

        for (ResultPartitionWriter partition : producedPartitions) {
            //方法中会创建LocalBufferPool
            partition.setup();
        }

        // InputGates must be initialized after the partitions, since during InputGate#setup
        // we are requesting partitions
        //InputGates必须在分区之后初始化，因为在InputGatesetup期间，我们正在请求分区
        for (InputGate gate : inputGates) {
            gate.setup();
        }
    }

    /**
     * Releases resources before task exits. We should also fail the partition to release if the
     * task has failed, is canceled, or is being canceled at the moment.
     */
    //任务退出前释放资源。如果任务失败、被取消或正在被取消，我们还应该使分区无法释放。
    private void releaseResources() {
        LOG.debug(
                "Release task {} network resources (state: {}).",
                taskNameWithSubtask,
                getExecutionState());

        for (ResultPartitionWriter partitionWriter : partitionWriters) {
            taskEventDispatcher.unregisterPartition(partitionWriter.getPartitionId());
        }

        // close network resources
        //关闭网络资源
        if (isCanceledOrFailed()) {
            failAllResultPartitions();
        }
        closeAllResultPartitions();
        closeAllInputGates();

        try {
            taskStateManager.close();
        } catch (Exception e) {
            LOG.error("Failed to close task state manager for task {}.", taskNameWithSubtask, e);
        }
    }

    private void failAllResultPartitions() {
        for (ResultPartitionWriter partitionWriter : partitionWriters) {
            partitionWriter.fail(getFailureCause());
        }
    }

    private void closeAllResultPartitions() {
        for (ResultPartitionWriter partitionWriter : partitionWriters) {
            try {
                partitionWriter.close();
            } catch (Throwable t) {
                ExceptionUtils.rethrowIfFatalError(t);
                LOG.error(
                        "Failed to release result partition for task {}.", taskNameWithSubtask, t);
            }
        }
    }

    private void closeAllInputGates() {
        TaskInvokable invokable = this.invokable;
        if (invokable == null || !invokable.isUsingNonBlockingInput()) {
            // Cleanup resources instead of invokable if it is null, or prevent it from being
            // blocked on input, or interrupt if it is already blocked. Not needed for StreamTask
            // (which does NOT use blocking input); for which this could cause race conditions
            //如果为空，则清理资源而不是可调用，或者防止其在输入时被阻塞，或者如果已经被阻塞则中断。
            //StreamTask 不需要（它不使用阻塞输入）；这可能会导致竞争条件
            for (InputGate inputGate : inputGates) {
                try {
                    inputGate.close();
                } catch (Throwable t) {
                    ExceptionUtils.rethrowIfFatalError(t);
                    LOG.error("Failed to release input gate for task {}.", taskNameWithSubtask, t);
                }
            }
        }
    }

    //创建用户代码类加载器
    private UserCodeClassLoader createUserCodeClassloader() throws Exception {
        long startDownloadTime = System.currentTimeMillis();

        // triggers the download of all missing jar files from the job manager
        //触发从作业管理器下载所有丢失的 jar 文件
        final UserCodeClassLoader userCodeClassLoader =
                classLoaderHandle.getOrResolveClassLoader(requiredJarFiles, requiredClasspaths);

        LOG.debug(
                "Getting user code class loader for task {} at library cache manager took {} milliseconds",
                executionId,
                System.currentTimeMillis() - startDownloadTime);

        return userCodeClassLoader;
    }

    private void notifyFinalState() {
        checkState(executionState.isTerminal());
        taskManagerActions.updateTaskExecutionState(
                new TaskExecutionState(executionId, executionState, failureCause));
    }

    private void notifyFatalError(String message, Throwable cause) {
        taskManagerActions.notifyFatalError(message, cause);
    }

    /**
     * Try to transition the execution state from the current state to the new state.
     *
     * @param currentState of the execution
     * @param newState of the execution
     * @return true if the transition was successful, otherwise false
     */
    //尝试将执行状态从当前状态转换到新状态。
    private boolean transitionState(ExecutionState currentState, ExecutionState newState) {
        return transitionState(currentState, newState, null);
    }

    /**
     * Try to transition the execution state from the current state to the new state.
     *
     * @param currentState of the execution
     * @param newState of the execution
     * @param cause of the transition change or null
     * @return true if the transition was successful, otherwise false
     */
    //尝试将执行状态从当前状态转换到新状态。
    private boolean transitionState(
            ExecutionState currentState, ExecutionState newState, Throwable cause) {
        if (STATE_UPDATER.compareAndSet(this, currentState, newState)) {
            if (cause == null) {
                LOG.info(
                        "{} ({}) switched from {} to {}.",
                        taskNameWithSubtask,
                        executionId,
                        currentState,
                        newState);
            } else if (ExceptionUtils.findThrowable(cause, CancelTaskException.class).isPresent()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "{} ({}) switched from {} to {} due to CancelTaskException:",
                            taskNameWithSubtask,
                            executionId,
                            currentState,
                            newState,
                            cause);
                } else {
                    LOG.info(
                            "{} ({}) switched from {} to {} due to CancelTaskException.",
                            taskNameWithSubtask,
                            executionId,
                            currentState,
                            newState);
                }
            } else {
                // proper failure of the task. record the exception as the root
                // cause
                //任务的正确失败。将异常记录为根本原因
                failureCause = cause;
                LOG.warn(
                        "{} ({}) switched from {} to {} with failure cause:",
                        taskNameWithSubtask,
                        executionId,
                        currentState,
                        newState,
                        cause);
            }

            return true;
        } else {
            return false;
        }
    }

    // ----------------------------------------------------------------------------------------------------------------
    //  Canceling / Failing the task from the outside
    // ----------------------------------------------------------------------------------------------------------------
    //从外部取消失败的任务

    /**
     * Cancels the task execution. If the task is already in a terminal state (such as FINISHED,
     * CANCELED, FAILED), or if the task is already canceling this does nothing. Otherwise it sets
     * the state to CANCELING, and, if the invokable code is running, starts an asynchronous thread
     * that aborts that code.
     *
     * <p>This method never blocks.
     */
    //取消任务执行。
    //如果任务已处于最终状态（例如 FINISHED、CANCELED、FAILED），或者任务已在取消，则不会执行任何操作。
    //否则，它将状态设置为 CANCELING，并且如果可调用代码正在运行，则启动一个异步线程来中止该代码。
    //此方法永远不会阻塞
    public void cancelExecution() {
        try (MdcUtils.MdcCloseable ignored = MdcUtils.withContext(MdcUtils.asContextData(jobId))) {
            LOG.info("Attempting to cancel task {} ({}).", taskNameWithSubtask, executionId);
            cancelOrFailAndCancelInvokable(ExecutionState.CANCELING, null);
        }
    }

    /**
     * Marks task execution failed for an external reason (a reason other than the task code itself
     * throwing an exception). If the task is already in a terminal state (such as FINISHED,
     * CANCELED, FAILED), or if the task is already canceling this does nothing. Otherwise it sets
     * the state to FAILED, and, if the invokable code is running, starts an asynchronous thread
     * that aborts that code.
     *
     * <p>This method never blocks.
     */
    //标记任务执行因外部原因（任务代码本身抛出异常以外的原因）失败。
    //如果任务已处于最终状态（例如 FINISHED、CANCELED、FAILED），或者任务已在取消，则不会执行任何操作。
    //否则，它将状态设置为 FAILED，并且如果可调用代码正在运行，则启动一个异步线程来中止该代码。
    //此方法永远不会阻塞
    @Override
    public void failExternally(Throwable cause) {
        try (MdcUtils.MdcCloseable ignored = MdcUtils.withContext(MdcUtils.asContextData(jobId))) {
            LOG.info(
                    "Attempting to fail task externally {} ({}).",
                    taskNameWithSubtask,
                    executionId);
            cancelOrFailAndCancelInvokable(ExecutionState.FAILED, cause);
        }
    }

    private void cancelOrFailAndCancelInvokable(ExecutionState targetState, Throwable cause) {
        try {
            cancelOrFailAndCancelInvokableInternal(targetState, cause);
        } catch (Throwable t) {
            if (ExceptionUtils.isJvmFatalOrOutOfMemoryError(t)) {
                String message =
                        String.format(
                                "FATAL - exception in cancelling task %s (%s).",
                                taskNameWithSubtask, executionId);
                notifyFatalError(message, t);
            } else {
                throw t;
            }
        }
    }

    @VisibleForTesting
    void cancelOrFailAndCancelInvokableInternal(ExecutionState targetState, Throwable cause) {
        cause = preProcessException(cause);

        while (true) {
            ExecutionState current = executionState;

            // if the task is already canceled (or canceling) or finished or failed,
            // then we need not do anything
            //如果任务已经被取消（或正在取消）或完成或失败，那么我们不需要做任何事情
            if (current.isTerminal() || current == ExecutionState.CANCELING) {
                LOG.info("Task {} is already in state {}", taskNameWithSubtask, current);
                return;
            }

            if (current == ExecutionState.DEPLOYING || current == ExecutionState.CREATED) {
                if (transitionState(current, targetState, cause)) {
                    // if we manage this state transition, then the invokable gets never called
                    // we need not call cancel on it
                    //如果我们管理此状态转换，则可调用的对象永远不会被调用，我们无需对其调用取消
                    return;
                }
            } else if (current == ExecutionState.INITIALIZING
                    || current == ExecutionState.RUNNING) {
                if (transitionState(current, targetState, cause)) {
                    // we are canceling / failing out of the running state
                    // we need to cancel the invokable
                    //我们正在取消运行状态失败，我们需要取消可调用

                    // copy reference to guard against concurrent null-ing out the reference
                    //复制引用以防止并发清空引用
                    final TaskInvokable invokable = this.invokable;

                    if (invokable != null && invokableHasBeenCanceled.compareAndSet(false, true)) {
                        LOG.info(
                                "Triggering cancellation of task code {} ({}).",
                                taskNameWithSubtask,
                                executionId);

                        // because the canceling may block on user code, we cancel from a separate
                        // thread
                        // we do not reuse the async call handler, because that one may be blocked,
                        // in which
                        // case the canceling could not continue
                        //因为取消可能会阻塞用户代码，所以我们从一个单独的线程中取消，
                        //我们不会重用异步调用处理程序，因为该处理程序可能会被阻塞，在这种情况下取消无法继续

                        // The canceller calls cancel and interrupts the executing thread once
                        //取消程序调用cancel并中断正在执行的线程一次
                        Runnable canceler =
                                new TaskCanceler(
                                        LOG, invokable, executingThread, taskNameWithSubtask);

                        Thread cancelThread =
                                new Thread(
                                        executingThread.getThreadGroup(),
                                        canceler,
                                        String.format(
                                                "Canceler for %s (%s).",
                                                taskNameWithSubtask, executionId));
                        cancelThread.setDaemon(true);
                        cancelThread.setUncaughtExceptionHandler(
                                FatalExitExceptionHandler.INSTANCE);
                        cancelThread.start();

                        // the periodic interrupting thread - a different thread than the canceller,
                        // in case
                        // the application code does blocking stuff in its cancellation paths.
                        //周期性中断线程 - 与取消程序不同的线程，以防应用程序代码在其取消路径中阻塞某些内容。
                        Runnable interrupter =
                                new TaskInterrupter(
                                        LOG,
                                        invokable,
                                        executingThread,
                                        taskNameWithSubtask,
                                        taskCancellationInterval,
                                        jobId);

                        Thread interruptingThread =
                                new Thread(
                                        executingThread.getThreadGroup(),
                                        interrupter,
                                        String.format(
                                                "Canceler/Interrupts for %s (%s).",
                                                taskNameWithSubtask, executionId));
                        interruptingThread.setDaemon(true);
                        interruptingThread.setUncaughtExceptionHandler(
                                FatalExitExceptionHandler.INSTANCE);
                        interruptingThread.start();

                        // if a cancellation timeout is set, the watchdog thread kills the process
                        // if graceful cancellation does not succeed
                        //如果设置了取消超时，如果优雅取消未成功，看门狗线程将终止该进程
                        if (taskCancellationTimeout > 0) {
                            Runnable cancelWatchdog =
                                    new TaskCancelerWatchDog(
                                            taskInfo,
                                            executingThread,
                                            taskManagerActions,
                                            taskCancellationTimeout,
                                            jobId);

                            Thread watchDogThread =
                                    new Thread(
                                            executingThread.getThreadGroup(),
                                            cancelWatchdog,
                                            String.format(
                                                    "Cancellation Watchdog for %s (%s).",
                                                    taskNameWithSubtask, executionId));
                            watchDogThread.setDaemon(true);
                            watchDogThread.setUncaughtExceptionHandler(
                                    FatalExitExceptionHandler.INSTANCE);
                            watchDogThread.start();
                        }
                    }
                    return;
                }
            } else {
                throw new IllegalStateException(
                        String.format(
                                "Unexpected state: %s of task %s (%s).",
                                current, taskNameWithSubtask, executionId));
            }
        }
    }

    // ------------------------------------------------------------------------
    //  Partition State Listeners
    // ------------------------------------------------------------------------
    //分区状态监听器

    @Override
    public void requestPartitionProducerState(
            final IntermediateDataSetID intermediateDataSetId,
            final ResultPartitionID resultPartitionId,
            Consumer<? super ResponseHandle> responseConsumer) {

        final CompletableFuture<ExecutionState> futurePartitionState =
                partitionProducerStateChecker.requestPartitionProducerState(
                        jobId, intermediateDataSetId, resultPartitionId);

        FutureUtils.assertNoException(
                futurePartitionState
                        .handle(PartitionProducerStateResponseHandle::new)
                        .thenAcceptAsync(responseConsumer, executor));
    }

    // ------------------------------------------------------------------------
    //  Notifications on the invokable
    // ------------------------------------------------------------------------
    //关于可调用的通知

    /**
     * Calls the invokable to trigger a checkpoint.
     *
     * @param checkpointID The ID identifying the checkpoint.
     * @param checkpointTimestamp The timestamp associated with the checkpoint.
     * @param checkpointOptions Options for performing this checkpoint.
     */
    //调用invokable以触发检查点。
    public void triggerCheckpointBarrier(
            final long checkpointID,
            final long checkpointTimestamp,
            final CheckpointOptions checkpointOptions) {

        final TaskInvokable invokable = this.invokable;
        final CheckpointMetaData checkpointMetaData =
                new CheckpointMetaData(
                        checkpointID, checkpointTimestamp, System.currentTimeMillis());

        if (executionState == ExecutionState.RUNNING) {
            checkState(invokable instanceof CheckpointableTask, "invokable is not checkpointable");
            try {
                ((CheckpointableTask) invokable)
                        //触发器检查点异步
                        .triggerCheckpointAsync(checkpointMetaData, checkpointOptions)
                        .handle(
                                (triggerResult, exception) -> {
                                    if (exception != null || !triggerResult) {
                                        declineCheckpoint(
                                                checkpointID,
                                                CheckpointFailureReason.TASK_FAILURE,
                                                exception);
                                        return false;
                                    }
                                    return true;
                                });
            } catch (RejectedExecutionException ex) {
                // This may happen if the mailbox is closed. It means that the task is shutting
                // down, so we just ignore it.
                //如果mailbox关闭，则可能会发生这种情况。这意味着任务正在关闭，所以我们忽略它。
                LOG.debug(
                        "Triggering checkpoint {} for {} ({}) was rejected by the mailbox",
                        checkpointID,
                        taskNameWithSubtask,
                        executionId);
                declineCheckpoint(
                        checkpointID, CheckpointFailureReason.CHECKPOINT_DECLINED_TASK_CLOSING);
            } catch (Throwable t) {
                if (getExecutionState() == ExecutionState.RUNNING) {
                    failExternally(
                            new Exception(
                                    "Error while triggering checkpoint "
                                            + checkpointID
                                            + " for "
                                            + taskNameWithSubtask,
                                    t));
                } else {
                    LOG.debug(
                            "Encountered error while triggering checkpoint {} for "
                                    + "{} ({}) while being not in state running.",
                            checkpointID,
                            taskNameWithSubtask,
                            executionId,
                            t);
                }
            }
        } else {
            LOG.debug(
                    "Declining checkpoint request for non-running task {} ({}).",
                    taskNameWithSubtask,
                    executionId);

            // send back a message that we did not do the checkpoint
            //发回一条消息，我们没有做检查点
            declineCheckpoint(
                    checkpointID, CheckpointFailureReason.CHECKPOINT_DECLINED_TASK_NOT_READY);
        }
    }

    private void declineCheckpoint(long checkpointID, CheckpointFailureReason failureReason) {
        declineCheckpoint(checkpointID, failureReason, null);
    }

    private void declineCheckpoint(
            long checkpointID,
            CheckpointFailureReason failureReason,
            @Nullable Throwable failureCause) {
        checkpointResponder.declineCheckpoint(
                jobId,
                executionId,
                checkpointID,
                new CheckpointException(
                        "Task name with subtask : " + taskNameWithSubtask,
                        failureReason,
                        failureCause));
    }

    public void notifyCheckpointComplete(final long checkpointID) {
        notifyCheckpoint(
                checkpointID,
                CheckpointStoreUtil.INVALID_CHECKPOINT_ID,
                NotifyCheckpointOperation.COMPLETE);
    }

    public void notifyCheckpointAborted(
            final long checkpointID, final long latestCompletedCheckpointId) {
        notifyCheckpoint(
                checkpointID, latestCompletedCheckpointId, NotifyCheckpointOperation.ABORT);
    }

    public void notifyCheckpointSubsumed(long checkpointID) {
        notifyCheckpoint(
                checkpointID,
                CheckpointStoreUtil.INVALID_CHECKPOINT_ID,
                NotifyCheckpointOperation.SUBSUME);
    }

    private void notifyCheckpoint(
            long checkpointId,
            long latestCompletedCheckpointId,
            NotifyCheckpointOperation notifyCheckpointOperation) {
        TaskInvokable invokable = this.invokable;

        if (executionState == ExecutionState.RUNNING && invokable != null) {
            checkState(invokable instanceof CheckpointableTask, "invokable is not checkpointable");
            try {
                switch (notifyCheckpointOperation) {
                    case ABORT:
                        ((CheckpointableTask) invokable)
                                .notifyCheckpointAbortAsync(
                                        checkpointId, latestCompletedCheckpointId);
                        break;
                    case COMPLETE:
                        ((CheckpointableTask) invokable)
                                //通知检查点完成异步
                                .notifyCheckpointCompleteAsync(checkpointId);
                        break;
                    case SUBSUME:
                        ((CheckpointableTask) invokable)
                                .notifyCheckpointSubsumedAsync(checkpointId);
                }
            } catch (RejectedExecutionException ex) {
                // This may happen if the mailbox is closed. It means that the task is shutting
                // down, so we just ignore it.
                //如果mailbox关闭，则可能会发生这种情况。这意味着任务正在关闭，所以我们忽略它。
                LOG.debug(
                        "Notify checkpoint {}} {} for {} ({}) was rejected by the mailbox.",
                        notifyCheckpointOperation,
                        checkpointId,
                        taskNameWithSubtask,
                        executionId);
            } catch (Throwable t) {
                switch (notifyCheckpointOperation) {
                    case ABORT:
                    case COMPLETE:
                        if (getExecutionState() == ExecutionState.RUNNING) {
                            failExternally(
                                    new RuntimeException(
                                            String.format(
                                                    "Error while notify checkpoint %s.",
                                                    notifyCheckpointOperation),
                                            t));
                        }
                        break;
                    case SUBSUME:
                        // just rethrow the throwable out as we do not expect notification of
                        // subsume could fail the task.
                        //只是重新抛出可抛出的东西，因为我们不希望 subsume 的通知会使任务失败。
                        ExceptionUtils.rethrow(t);
                }
            }
        } else {
            LOG.info(
                    "Ignoring checkpoint {} notification for non-running task {}.",
                    notifyCheckpointOperation,
                    taskNameWithSubtask);
        }
    }

    /**
     * Dispatches an operator event to the invokable task.
     *
     * <p>If the event delivery did not succeed, this method throws an exception. Callers can use
     * that exception for error reporting, but need not react with failing this task (this method
     * takes care of that).
     *
     * @throws FlinkException This method throws exceptions indicating the reason why delivery did
     *     not succeed.
     */
    //将操作员事件分派给可调用任务。
    //如果事件传递未成功，此方法将引发异常。调用者可以使用该异常进行错误报告，但无需对此任务失败做出反应（此方法负责处理该问题）。
    public void deliverOperatorEvent(OperatorID operator, SerializedValue<OperatorEvent> evt)
            throws FlinkException {
        final TaskInvokable invokable = this.invokable;
        final ExecutionState currentState = this.executionState;

        if (invokable == null
                || (currentState != ExecutionState.RUNNING
                        && currentState != ExecutionState.INITIALIZING)) {
            throw new TaskNotRunningException("Task is not running, but in state " + currentState);
        }

        if (invokable instanceof CoordinatedTask) {
            try {
                ((CoordinatedTask) invokable).dispatchOperatorEvent(operator, evt);
            } catch (Throwable t) {
                ExceptionUtils.rethrowIfFatalErrorOrOOM(t);

                if (getExecutionState() == ExecutionState.RUNNING
                        || getExecutionState() == ExecutionState.INITIALIZING) {
                    FlinkException e = new FlinkException("Error while handling operator event", t);
                    failExternally(e);
                    throw e;
                }
            }
        }
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    private void cancelInvokable(TaskInvokable invokable) {
        // in case of an exception during execution, we still call "cancel()" on the task
        //如果执行过程中出现异常，我们仍然对任务调用“cancel()”
        if (invokable != null && invokableHasBeenCanceled.compareAndSet(false, true)) {
            try {
                invokable.cancel();
            } catch (Throwable t) {
                LOG.error("Error while canceling task {}.", taskNameWithSubtask, t);
            }
        }
    }

    @Override
    public String toString() {
        return String.format("%s (%s) [%s]", taskNameWithSubtask, executionId, executionState);
    }

    @VisibleForTesting
    class PartitionProducerStateResponseHandle implements ResponseHandle {
        private final Either<ExecutionState, Throwable> result;

        PartitionProducerStateResponseHandle(
                @Nullable ExecutionState producerState, @Nullable Throwable t) {
            this.result = producerState != null ? Either.Left(producerState) : Either.Right(t);
        }

        @Override
        public ExecutionState getConsumerExecutionState() {
            return executionState;
        }

        @Override
        public Either<ExecutionState, Throwable> getProducerExecutionState() {
            return result;
        }

        @Override
        public void cancelConsumption() {
            cancelExecution();
        }

        @Override
        public void failConsumption(Throwable cause) {
            failExternally(cause);
        }
    }

    /**
     * Instantiates the given task invokable class, passing the given environment (and possibly the
     * initial task state) to the task's constructor.
     *
     * <p>The method will first try to instantiate the task via a constructor accepting both the
     * Environment and the TaskStateSnapshot. If no such constructor exists, and there is no initial
     * state, the method will fall back to the stateless convenience constructor that accepts only
     * the Environment.
     *
     * @param classLoader The classloader to load the class through.
     * @param className The name of the class to load.
     * @param environment The task environment.
     * @return The instantiated invokable task object.
     * @throws Throwable Forwards all exceptions that happen during initialization of the task. Also
     *     throws an exception if the task class misses the necessary constructor.
     */
    //实例化给定的任务invokable类，将给定的环境 (可能还有初始任务状态) 传递给任务的构造函数。
    //该方法将首先尝试通过接受环境和TaskStateSnapshot的构造函数来实例化任务。
    //如果不存在这样的构造函数，并且没有初始状态，则该方法将回退到仅接受环境的无状态便利构造函数。
    //参数:
    //classLoader -用于加载类的类加载器。
    //className -要加载的类的名称。
    //environment -任务环境。
    private static TaskInvokable loadAndInstantiateInvokable(
            ClassLoader classLoader, String className, Environment environment) throws Throwable {

        final Class<? extends TaskInvokable> invokableClass;
        try {
            invokableClass =
                    Class.forName(className, true, classLoader).asSubclass(TaskInvokable.class);
        } catch (Throwable t) {
            throw new Exception("Could not load the task's invokable class.", t);
        }

        Constructor<? extends TaskInvokable> statelessCtor;

        try {
            //获取只包含Environment入参类型的StreamTask构造函数
            statelessCtor = invokableClass.getConstructor(Environment.class);
        } catch (NoSuchMethodException ee) {
            throw new FlinkException("Task misses proper constructor", ee);
        }

        // instantiate the class
        try {
            //noinspection ConstantConditions  --> cannot happen
            //新建StreamTask实例。
            return statelessCtor.newInstance(environment);
        } catch (InvocationTargetException e) {
            // directly forward exceptions from the eager initialization
            throw e.getTargetException();
        } catch (Exception e) {
            throw new FlinkException("Could not instantiate the task's invokable class.", e);
        }
    }

    // ------------------------------------------------------------------------
    //  Task cancellation
    //
    //  The task cancellation uses in total three threads, as a safety net
    //  against various forms of user- and JVM bugs.
    //
    //    - The first thread calls 'cancel()' on the invokable and closes
    //      the input and output connections, for fast thread termination
    //    - The second thread periodically interrupts the invokable in order
    //      to pull the thread out of blocking wait and I/O operations
    //    - The third thread (watchdog thread) waits until the cancellation
    //      timeout and then performs a hard cancel (kill process, or let
    //      the TaskManager know)
    //
    //  Previously, thread two and three were in one thread, but we needed
    //  to separate this to make sure the watchdog thread does not call
    //  'interrupt()'. This is a workaround for the following JVM bug
    //   https://bugs.java.com/bugdatabase/view_bug.do?bug_id=8138622
    // ------------------------------------------------------------------------
    //任务取消
    //任务取消总共使用三个线程，作为针对各种形式的用户和 JVM 错误的安全网。
    // - 第一个线程对可调用对象调用“cancel()”，并关闭输入和输出连接，以实现快速线程终止
    // - 第二个线程定期中断可调用对象，以便将线程从阻塞等待和 IO 操作中拉出
    // - 第三个线程（看门狗线程）等待取消超时，然后执行硬取消（杀死进程，或让 TaskManager 知道）
    // 以前，线程二和三在一个线程中，但我们需要将其分开以确保看门狗线程不会调用“中断()”。
    //这是针对以下 JVM 错误的解决方法 https:bugs.java.combugdatabaseview_bug.do?bug_id=8138622

    /**
     * This runner calls cancel() on the invokable, closes input-/output resources, and initially
     * interrupts the task thread.
     */
    //该运行程序对可调用对象调用 cancel()，关闭输入/ 输出资源，并首先中断任务线程。
    private class TaskCanceler implements Runnable {

        private final Logger logger;
        private final TaskInvokable invokable;
        private final Thread executor;
        private final String taskName;

        TaskCanceler(Logger logger, TaskInvokable invokable, Thread executor, String taskName) {
            this.logger = logger;
            this.invokable = invokable;
            this.executor = executor;
            this.taskName = taskName;
        }

        @Override
        public void run() {
            try (MdcCloseable ignored = MdcUtils.withContext(MdcUtils.asContextData(jobId))) {
                // the user-defined cancel method may throw errors.
                // we need do continue despite that
                //用户定义的取消方法可能会抛出错误。尽管如此，我们仍需要继续
                try {
                    invokable.cancel();
                } catch (Throwable t) {
                    ExceptionUtils.rethrowIfFatalError(t);
                    logger.error("Error while canceling the task {}.", taskName, t);
                }

                // Early release of input and output buffer pools. We do this
                // in order to unblock async Threads, which produce/consume the
                // intermediate streams outside of the main Task Thread (like
                // the Kafka consumer).
                // Notes: 1) This does not mean to release all network resources,
                // the task thread itself will release them; 2) We can not close
                // ResultPartitions here because of possible race conditions with
                // Task thread so we just call the fail here.
                //提前释放输入和输出缓冲池。
                //我们这样做是为了解除异步线程的阻塞，异步线程会消耗主任务线程之外的中间流（如 Kafka 消费者）。
                // 注意：
                // 1）这并不意味着释放所有网络资源，任务线程本身会释放它们；
                // 2）我们不能在这里关闭 ResultPartitions，因为任务线程可能存在竞争条件，所以我们在这里调用失败。
                failAllResultPartitions();
                closeAllInputGates();

                invokable.maybeInterruptOnCancel(executor, null, null);
            } catch (Throwable t) {
                ExceptionUtils.rethrowIfFatalError(t);
                logger.error("Error in the task canceler for task {}.", taskName, t);
            }
        }
    }

    /** This thread sends the delayed, periodic interrupt calls to the executing thread. */
    //该线程向执行线程发送延迟的周期性中断调用。
    private static final class TaskInterrupter implements Runnable {

        /** The logger to report on the fatal condition. */
        //记录器报告致命情况。
        private final Logger log;

        /** The invokable task. */
        //可调用的任务。
        private final TaskInvokable task;

        /** The executing task thread that we wait for to terminate. */
        //我们等待终止的执行任务线程。
        private final Thread executorThread;

        /** The name of the task, for logging purposes. */
        //任务的名称，用于记录目的。
        private final String taskName;

        /** The interval in which we interrupt. */
        //我们中断的时间间隔。
        private final long interruptIntervalMillis;

        private final JobID jobID;

        TaskInterrupter(
                Logger log,
                TaskInvokable task,
                Thread executorThread,
                String taskName,
                long interruptIntervalMillis,
                JobID jobID) {

            this.log = log;
            this.task = task;
            this.executorThread = executorThread;
            this.taskName = taskName;
            this.interruptIntervalMillis = interruptIntervalMillis;
            this.jobID = jobID;
        }

        @Override
        public void run() {
            try (MdcCloseable ignored = MdcUtils.withContext(MdcUtils.asContextData(jobID))) {
                // we initially wait for one interval
                // in most cases, the threads go away immediately (by the cancellation thread)
                // and we need not actually do anything
                //在大多数情况下，我们一开始会等待一个时间间隔，线程会立即消失（通过取消线程），并且我们实际上不需要执行任何操作
                executorThread.join(interruptIntervalMillis);

                // log stack trace where the executing thread is stuck and
                // interrupt the running thread periodically while it is still alive
                //记录执行线程被卡住的堆栈跟踪，并在运行线程仍处于活动状态时定期中断它
                while (executorThread.isAlive()) {
                    task.maybeInterruptOnCancel(executorThread, taskName, interruptIntervalMillis);
                    try {
                        executorThread.join(interruptIntervalMillis);
                    } catch (InterruptedException e) {
                        // we ignore this and fall through the loop
                        //我们忽略这一点并陷入循环
                    }
                }
            } catch (Throwable t) {
                ExceptionUtils.rethrowIfFatalError(t);
                log.error("Error in the task canceler for task {}.", taskName, t);
            }
        }
    }

    /**
     * Watchdog for the cancellation. If the task thread does not go away gracefully within a
     * certain time, we trigger a hard cancel action (notify TaskManager of fatal error, which in
     * turn kills the process).
     */
    //取消的看门狗。如果任务线程在一定时间内没有正常消失，我们会触发硬取消操作（通知 TaskManager 致命错误，从而终止进程）
    private static class TaskCancelerWatchDog implements Runnable {

        /** The executing task thread that we wait for to terminate. */
        //我们等待终止的执行任务线程。
        private final Thread executorThread;

        /** The TaskManager to notify if cancellation does not happen in time. */
        //如果取消没有及时发生，TaskManager 会发出通知。
        private final TaskManagerActions taskManager;

        /** The timeout for cancellation. */
        //取消超时。
        private final long timeoutMillis;

        private final TaskInfo taskInfo;

        private final JobID jobID;

        TaskCancelerWatchDog(
                TaskInfo taskInfo,
                Thread executorThread,
                TaskManagerActions taskManager,
                long timeoutMillis,
                JobID jobID) {

            checkArgument(timeoutMillis > 0);

            this.taskInfo = taskInfo;
            this.executorThread = executorThread;
            this.taskManager = taskManager;
            this.timeoutMillis = timeoutMillis;
            this.jobID = jobID;
        }

        @Override
        public void run() {
            try (MdcCloseable ign = MdcUtils.withContext(MdcUtils.asContextData(jobID))) {
                Deadline timeout = Deadline.fromNow(Duration.ofMillis(timeoutMillis));
                while (executorThread.isAlive() && timeout.hasTimeLeft()) {
                    try {
                        executorThread.join(Math.max(1, timeout.timeLeft().toMillis()));
                    } catch (InterruptedException ignored) {
                        // we don't react to interrupted exceptions, simply fall through the loop
                        //我们不会对中断的异常做出反应，只是陷入循环
                    }
                }

                if (executorThread.isAlive()) {
                    logTaskThreadStackTrace(
                            executorThread,
                            taskInfo.getTaskNameWithSubtasks(),
                            timeoutMillis,
                            "notifying TM");
                    String msg =
                            "Task did not exit gracefully within "
                                    + (timeoutMillis / 1000)
                                    + " + seconds.";
                    taskManager.notifyFatalError(msg, new FlinkRuntimeException(msg));
                }
            } catch (Throwable t) {
                throw new FlinkRuntimeException("Error in Task Cancellation Watch Dog", t);
            }
        }
    }

    public static void logTaskThreadStackTrace(
            Thread thread, String taskName, long timeoutMs, String action) {
        StackTraceElement[] stack = thread.getStackTrace();
        StringBuilder stackTraceStr = new StringBuilder();
        for (StackTraceElement e : stack) {
            stackTraceStr.append(e).append('\n');
        }

        LOG.warn(
                "Task '{}' did not react to cancelling signal - {}; it is stuck for {} seconds in method:\n {}",
                taskName,
                action,
                timeoutMs / 1000,
                stackTraceStr);
    }

    /** Various operation of notify checkpoint. */
    //通知检查点的各种操作。
    public enum NotifyCheckpointOperation {
        ABORT,
        COMPLETE,
        SUBSUME
    }
}
