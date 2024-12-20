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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.api.writer.RecordWriterDelegate;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.InternalOperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.operators.coordination.AcknowledgeCheckpointEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventDispatcher;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.streaming.api.graph.NonChainedOutput;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.CountingOutput;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.SourceOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactoryUtil;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.runtime.io.RecordWriterOutput;
import org.apache.flink.streaming.runtime.io.StreamTaskSourceInput;
import org.apache.flink.streaming.runtime.operators.sink.SinkWriterOperatorFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxExecutorFactory;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.SerializedValue;

import org.apache.flink.shaded.guava31.com.google.common.io.Closer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The {@code OperatorChain} contains all operators that are executed as one chain within a single
 * {@link StreamTask}.
 *
 * <p>The main entry point to the chain is it's {@code mainOperator}. {@code mainOperator} is
 * driving the execution of the {@link StreamTask}, by pulling the records from network inputs
 * and/or source inputs and pushing produced records to the remaining chained operators.
 *
 * @param <OUT> The type of elements accepted by the chain, i.e., the input type of the chain's main
 *     operator.
 */
//OperatorChain包含在单个StreamTask中作为一条链执行的所有运算符。
//链的主要入口点是它的mainOperator 。
// mainOperator通过从网络输入和/ 或源输入中提取记录并将生成的记录推送到其余的链式运算符来驱动StreamTask的执行。
public abstract class OperatorChain<OUT, OP extends StreamOperator<OUT>>
        implements BoundedMultiInput, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(OperatorChain.class);

    protected final RecordWriterOutput<?>[] streamOutputs;

    protected final WatermarkGaugeExposingOutput<StreamRecord<OUT>> mainOperatorOutput;

    /**
     * For iteration, {@link StreamIterationHead} and {@link StreamIterationTail} used for executing
     * feedback edges do not contain any operators, in which case, {@code mainOperatorWrapper} and
     * {@code tailOperatorWrapper} are null.
     *
     * <p>Usually first operator in the chain is the same as {@link #mainOperatorWrapper}, but
     * that's not the case if there are chained source inputs. In this case, one of the source
     * inputs will be the first operator. For example the following operator chain is possible:
     *
     * <pre>
     * first
     *      \
     *      main (multi-input) -> ... -> tail
     *      /
     * second
     * </pre>
     *
     * <p>Where "first" and "second" (there can be more) are chained source operators. When it comes
     * to things like closing, stat initialisation or state snapshotting, the operator chain is
     * traversed: first, second, main, ..., tail or in reversed order: tail, ..., main, second,
     * first
     */
    //对于迭代，用于执行反馈边的StreamIterationHead和StreamIterationTail不包含任何运算符，
    // 在这种情况下， mainOperatorWrapper和tailOperatorWrapper为空。
    //通常链中的第一个运算符与mainOperatorWrapper相同，但如果存在链接的源输入，则情况并非如此。
    // 在这种情况下，源输入之一将是第一个运算符。例如，以下操作符链是可能的：
    //  first
    //       \
    //       main (multi-input) -> ... -> tail
    //       /
    //  second
    //
    //其中“第一”和“第二”（可以有更多）是链式源运算符。
    // 当涉及到诸如关闭、统计初始化或状态快照之类的操作时，会遍历操作符链：first, secondary, main, ..., tail
    // 或按相反顺序：tail, ..., main, secondary,first
    @Nullable protected final StreamOperatorWrapper<OUT, OP> mainOperatorWrapper;

    @Nullable protected final StreamOperatorWrapper<?, ?> firstOperatorWrapper;
    @Nullable protected final StreamOperatorWrapper<?, ?> tailOperatorWrapper;

    protected final Map<StreamConfig.SourceInputConfig, ChainedSource> chainedSources;

    protected final int numOperators;

    protected final OperatorEventDispatcherImpl operatorEventDispatcher;

    protected final Closer closer = Closer.create();

    protected final @Nullable FinishedOnRestoreInput finishedOnRestoreInput;

    protected boolean isClosed;

    public OperatorChain(
            StreamTask<OUT, OP> containingTask,
            RecordWriterDelegate<SerializationDelegate<StreamRecord<OUT>>> recordWriterDelegate) {

        this.operatorEventDispatcher =
                new OperatorEventDispatcherImpl(
                        containingTask.getEnvironment().getUserCodeClassLoader().asClassLoader(),
                        containingTask.getEnvironment().getOperatorCoordinatorEventGateway());

        final ClassLoader userCodeClassloader = containingTask.getUserCodeClassLoader();
        final StreamConfig configuration = containingTask.getConfiguration();

        //算子工厂实例
        StreamOperatorFactory<OUT> operatorFactory =
                configuration.getStreamOperatorFactory(userCodeClassloader);

        // we read the chained configs, and the order of record writer registrations by output name
        //我们读取了链接的configs，以及按输出名称记录编写器注册的顺序
        //可chain的算子配置集合
        Map<Integer, StreamConfig> chainedConfigs =
                configuration.getTransitiveChainedTaskConfigsWithSelf(userCodeClassloader);

        // create the final output stream writers
        // we iterate through all the out edges from this job vertex and create a stream output
        //创建最终输出流编写器
        //我们遍历这个作业顶点的所有out边，并创建一个流输出
        List<NonChainedOutput> outputsInOrder =
                configuration.getVertexNonChainedOutputs(userCodeClassloader);
        Map<IntermediateDataSetID, RecordWriterOutput<?>> recordWriterOutputs =
                CollectionUtil.newHashMapWithExpectedSize(outputsInOrder.size());
        this.streamOutputs = new RecordWriterOutput<?>[outputsInOrder.size()];
        this.finishedOnRestoreInput =
                this.isTaskDeployedAsFinished()
                        ? new FinishedOnRestoreInput(
                                streamOutputs, configuration.getInputs(userCodeClassloader).length)
                        : null;

        // from here on, we need to make sure that the output writers are shut down again on failure
        // 从这里开始，我们需要确保输出写入器在失败时再次关闭
        boolean success = false;
        try {
            //创建链输出
            createChainOutputs(
                    outputsInOrder,
                    recordWriterDelegate,
                    chainedConfigs,
                    containingTask,
                    recordWriterOutputs);

            // we create the chain of operators and grab the collector that leads into the chain
            //我们创建操作符链，并抓住通向链的收集器
            List<StreamOperatorWrapper<?, ?>> allOpWrappers =
                    new ArrayList<>(chainedConfigs.size());
            this.mainOperatorOutput =
                    //负责创建整个OperatorChain中的算子以及算子输出。
                    createOutputCollector(
                            containingTask,
                            configuration,
                            chainedConfigs,
                            userCodeClassloader,
                            recordWriterOutputs,
                            allOpWrappers,
                            containingTask.getMailboxExecutorFactory(),
                            operatorFactory != null);

            if (operatorFactory != null) {
                Tuple2<OP, Optional<ProcessingTimeService>> mainOperatorAndTimeService =
                        StreamOperatorFactoryUtil.createOperator(
                                operatorFactory,
                                containingTask,
                                configuration,
                                mainOperatorOutput,
                                operatorEventDispatcher);

                OP mainOperator = mainOperatorAndTimeService.f0;
                mainOperator
                        .getMetricGroup()
                        .gauge(
                                MetricNames.IO_CURRENT_OUTPUT_WATERMARK,
                                mainOperatorOutput.getWatermarkGauge());
                this.mainOperatorWrapper =
                        createOperatorWrapper(
                                mainOperator,
                                containingTask,
                                configuration,
                                mainOperatorAndTimeService.f1,
                                true);

                // add main operator to end of chain
                //将主运算符添加到链的末尾
                allOpWrappers.add(mainOperatorWrapper);

                this.tailOperatorWrapper = allOpWrappers.get(0);
            } else {
                checkState(allOpWrappers.size() == 0);
                this.mainOperatorWrapper = null;
                this.tailOperatorWrapper = null;
            }

            this.chainedSources =
                    createChainedSources(
                            containingTask,
                            configuration.getInputs(userCodeClassloader),
                            chainedConfigs,
                            userCodeClassloader,
                            allOpWrappers);

            this.numOperators = allOpWrappers.size();

            firstOperatorWrapper = linkOperatorWrappers(allOpWrappers);

            success = true;
        } finally {
            // make sure we clean up after ourselves in case of a failure after acquiring
            // the first resources
            //确保我们在获取第一个资源后发生故障时自行清理
            if (!success) {
                for (int i = 0; i < streamOutputs.length; i++) {
                    if (streamOutputs[i] != null) {
                        streamOutputs[i].close();
                    }
                    streamOutputs[i] = null;
                }
            }
        }
    }

    @VisibleForTesting
    OperatorChain(
            List<StreamOperatorWrapper<?, ?>> allOperatorWrappers,
            RecordWriterOutput<?>[] streamOutputs,
            WatermarkGaugeExposingOutput<StreamRecord<OUT>> mainOperatorOutput,
            StreamOperatorWrapper<OUT, OP> mainOperatorWrapper) {
        this.streamOutputs = streamOutputs;
        this.finishedOnRestoreInput = null;
        this.mainOperatorOutput = checkNotNull(mainOperatorOutput);
        this.operatorEventDispatcher = null;

        checkState(allOperatorWrappers != null && allOperatorWrappers.size() > 0);
        this.mainOperatorWrapper = checkNotNull(mainOperatorWrapper);
        this.tailOperatorWrapper = allOperatorWrappers.get(0);
        this.numOperators = allOperatorWrappers.size();
        this.chainedSources = Collections.emptyMap();

        firstOperatorWrapper = linkOperatorWrappers(allOperatorWrappers);
    }

    public abstract boolean isTaskDeployedAsFinished();

    public abstract void dispatchOperatorEvent(
            OperatorID operator, SerializedValue<OperatorEvent> event) throws FlinkException;

    public abstract void prepareSnapshotPreBarrier(long checkpointId) throws Exception;

    /**
     * Ends the main operator input specified by {@code inputId}).
     *
     * @param inputId the input ID starts from 1 which indicates the first input.
     */
    //结束由inputId指定的主运算符输入。
    public abstract void endInput(int inputId) throws Exception;

    /**
     * Initialize state and open all operators in the chain from <b>tail to heads</b>, contrary to
     * {@link StreamOperator#close()} which happens <b>heads to tail</b> (see {@link
     * #finishOperators(StreamTaskActionExecutor, StopMode)}).
     */
    //初始化状态并打开从尾到头的链中的所有运算符，这与StreamOperator. close() 相反，
    //后者从头到尾发生 (请参见finishOperators(StreamTaskActionExecutor，StopMode)。
    public abstract void initializeStateAndOpenOperators(
            StreamTaskStateInitializer streamTaskStateInitializer) throws Exception;

    /**
     * Closes all operators in a chain effect way. Closing happens from <b>heads to tail</b>
     * operator in the chain, contrary to {@link StreamOperator#open()} which happens <b>tail to
     * heads</b> (see {@link #initializeStateAndOpenOperators(StreamTaskStateInitializer)}).
     */
    //以连锁效应方式关闭所有运营商。关闭发生在链中从头到尾运算符，这与StreamOperator. open()相反，
    // 后者发生在尾到头（请参阅initializeStateAndOpenOperators(StreamTaskStateInitializer) ）。
    public abstract void finishOperators(StreamTaskActionExecutor actionExecutor, StopMode stopMode)
            throws Exception;

    public abstract void notifyCheckpointComplete(long checkpointId) throws Exception;

    public abstract void notifyCheckpointAborted(long checkpointId) throws Exception;

    public abstract void notifyCheckpointSubsumed(long checkpointId) throws Exception;

    public abstract void snapshotState(
            Map<OperatorID, OperatorSnapshotFutures> operatorSnapshotsInProgress,
            CheckpointMetaData checkpointMetaData,
            CheckpointOptions checkpointOptions,
            Supplier<Boolean> isRunning,
            ChannelStateWriter.ChannelStateWriteResult channelStateWriteResult,
            CheckpointStreamFactory storage)
            throws Exception;

    public OperatorEventDispatcher getOperatorEventDispatcher() {
        return operatorEventDispatcher;
    }

    public void broadcastEvent(AbstractEvent event) throws IOException {
        broadcastEvent(event, false);
    }

    public void broadcastEvent(AbstractEvent event, boolean isPriorityEvent) throws IOException {
        for (RecordWriterOutput<?> streamOutput : streamOutputs) {
            streamOutput.broadcastEvent(event, isPriorityEvent);
        }
    }

    public void alignedBarrierTimeout(long checkpointId) throws IOException {
        for (RecordWriterOutput<?> streamOutput : streamOutputs) {
            streamOutput.alignedBarrierTimeout(checkpointId);
        }
    }

    public void abortCheckpoint(long checkpointId, CheckpointException cause) {
        for (RecordWriterOutput<?> streamOutput : streamOutputs) {
            streamOutput.abortCheckpoint(checkpointId, cause);
        }
    }

    /**
     * Execute {@link StreamOperator#close()} of each operator in the chain of this {@link
     * StreamTask}. Closing happens from <b>tail to head</b> operator in the chain.
     */
    //执行此StreamTask链中每个运算符的StreamOperator. close() 。关闭发生在链中从尾到头的操作符。
    public void closeAllOperators() throws Exception {
        isClosed = true;
    }

    public RecordWriterOutput<?>[] getStreamOutputs() {
        return streamOutputs;
    }

    /** Returns an {@link Iterable} which traverses all operators in forward topological order. */
    //返回一个Iterable ，它按正向拓扑顺序遍历所有运算符。
    @VisibleForTesting
    public Iterable<StreamOperatorWrapper<?, ?>> getAllOperators() {
        return getAllOperators(false);
    }

    /**
     * Returns an {@link Iterable} which traverses all operators in forward or reverse topological
     * order.
     */
    //返回以正向或反向拓扑顺序遍历所有运算符的Iterable。
    protected Iterable<StreamOperatorWrapper<?, ?>> getAllOperators(boolean reverse) {
        return reverse
                ? new StreamOperatorWrapper.ReadIterator(tailOperatorWrapper, true)
                : new StreamOperatorWrapper.ReadIterator(mainOperatorWrapper, false);
    }

    public Input getFinishedOnRestoreInputOrDefault(Input defaultInput) {
        return finishedOnRestoreInput == null ? defaultInput : finishedOnRestoreInput;
    }

    public int getNumberOfOperators() {
        return numOperators;
    }

    public WatermarkGaugeExposingOutput<StreamRecord<OUT>> getMainOperatorOutput() {
        return mainOperatorOutput;
    }

    public ChainedSource getChainedSource(StreamConfig.SourceInputConfig sourceInput) {
        checkArgument(
                chainedSources.containsKey(sourceInput),
                "Chained source with sourcedId = [%s] was not found",
                sourceInput);
        return chainedSources.get(sourceInput);
    }

    public List<Output<StreamRecord<?>>> getChainedSourceOutputs() {
        return chainedSources.values().stream()
                .map(ChainedSource::getSourceOutput)
                .collect(Collectors.toList());
    }

    public StreamTaskSourceInput<?> getSourceTaskInput(StreamConfig.SourceInputConfig sourceInput) {
        checkArgument(
                chainedSources.containsKey(sourceInput),
                "Chained source with sourcedId = [%s] was not found",
                sourceInput);
        return chainedSources.get(sourceInput).getSourceTaskInput();
    }

    public List<StreamTaskSourceInput<?>> getSourceTaskInputs() {
        return chainedSources.values().stream()
                .map(ChainedSource::getSourceTaskInput)
                .collect(Collectors.toList());
    }

    /**
     * This method should be called before finishing the record emission, to make sure any data that
     * is still buffered will be sent. It also ensures that all data sending related exceptions are
     * recognized.
     *
     * @throws IOException Thrown, if the buffered data cannot be pushed into the output streams.
     */
    //应在完成记录发送之前调用此方法，以确保发送仍在缓冲的任何数据。它还确保识别所有与数据发送相关的异常。
    public void flushOutputs() throws IOException {
        for (RecordWriterOutput<?> streamOutput : getStreamOutputs()) {
            streamOutput.flush();
        }
    }

    /**
     * This method releases all resources of the record writer output. It stops the output flushing
     * thread (if there is one) and releases all buffers currently held by the output serializers.
     *
     * <p>This method should never fail.
     */
    //此方法释放记录写入器输出的所有资源。它停止输出刷新线程（如果有）并释放输出序列化器当前持有的所有缓冲区。
    //这个方法永远不会失败。
    public void close() throws IOException {
        closer.close();
    }

    @Nullable
    public OP getMainOperator() {
        return (mainOperatorWrapper == null) ? null : mainOperatorWrapper.getStreamOperator();
    }

    @Nullable
    protected StreamOperator<?> getTailOperator() {
        return (tailOperatorWrapper == null) ? null : tailOperatorWrapper.getStreamOperator();
    }

    protected void snapshotChannelStates(
            StreamOperator<?> op,
            ChannelStateWriter.ChannelStateWriteResult channelStateWriteResult,
            OperatorSnapshotFutures snapshotInProgress) {
        if (op == getMainOperator()) {
            snapshotInProgress.setInputChannelStateFuture(
                    channelStateWriteResult
                            .getInputChannelStateHandles()
                            .thenApply(StateObjectCollection::new)
                            .thenApply(SnapshotResult::of));
        }
        if (op == getTailOperator()) {
            snapshotInProgress.setResultSubpartitionStateFuture(
                    channelStateWriteResult
                            .getResultSubpartitionStateHandles()
                            .thenApply(StateObjectCollection::new)
                            .thenApply(SnapshotResult::of));
        }
    }

    public boolean isClosed() {
        return isClosed;
    }

    /** Wrapper class to access the chained sources and their's outputs. */
    //用于访问链接源及其输出的包装类。
    public static class ChainedSource {
        private final WatermarkGaugeExposingOutput<StreamRecord<?>> chainedSourceOutput;
        private final StreamTaskSourceInput<?> sourceTaskInput;

        public ChainedSource(
                WatermarkGaugeExposingOutput<StreamRecord<?>> chainedSourceOutput,
                StreamTaskSourceInput<?> sourceTaskInput) {
            this.chainedSourceOutput = chainedSourceOutput;
            this.sourceTaskInput = sourceTaskInput;
        }

        public WatermarkGaugeExposingOutput<StreamRecord<?>> getSourceOutput() {
            return chainedSourceOutput;
        }

        public StreamTaskSourceInput<?> getSourceTaskInput() {
            return sourceTaskInput;
        }
    }

    // ------------------------------------------------------------------------
    //  initialization utilities
    // ------------------------------------------------------------------------

    private void createChainOutputs(
            List<NonChainedOutput> outputsInOrder,
            RecordWriterDelegate<SerializationDelegate<StreamRecord<OUT>>> recordWriterDelegate,
            Map<Integer, StreamConfig> chainedConfigs,
            StreamTask<OUT, OP> containingTask,
            Map<IntermediateDataSetID, RecordWriterOutput<?>> recordWriterOutputs) {
        for (int i = 0; i < outputsInOrder.size(); ++i) {
            NonChainedOutput output = outputsInOrder.get(i);

            RecordWriterOutput<?> recordWriterOutput =
                    //创建流输出
                    createStreamOutput(
                            recordWriterDelegate.getRecordWriter(i),
                            output,
                            chainedConfigs.get(output.getSourceNodeId()),
                            containingTask.getEnvironment());

            this.streamOutputs[i] = recordWriterOutput;
            recordWriterOutputs.put(output.getDataSetId(), recordWriterOutput);
        }
    }

    private RecordWriterOutput<OUT> createStreamOutput(
            RecordWriter<SerializationDelegate<StreamRecord<OUT>>> recordWriter,
            NonChainedOutput streamOutput,
            StreamConfig upStreamConfig,
            Environment taskEnvironment) {
        OutputTag sideOutputTag =
                //OutputTag，如果不是sideOutput，则返回null
                streamOutput.getOutputTag(); // OutputTag, return null if not sideOutput

        TypeSerializer outSerializer;

        if (streamOutput.getOutputTag() != null) {
            // side output
            //侧输出
            outSerializer =
                    upStreamConfig.getTypeSerializerSideOut(
                            streamOutput.getOutputTag(),
                            taskEnvironment.getUserCodeClassLoader().asClassLoader());
        } else {
            // main output
            //主输出
            outSerializer =
                    upStreamConfig.getTypeSerializerOut(
                            taskEnvironment.getUserCodeClassLoader().asClassLoader());
        }

        return closer.register(
                new RecordWriterOutput<OUT>(
                        recordWriter,
                        outSerializer,
                        sideOutputTag,
                        streamOutput.supportsUnalignedCheckpoints()));
    }

    @SuppressWarnings("rawtypes")
    private Map<StreamConfig.SourceInputConfig, ChainedSource> createChainedSources(
            StreamTask<OUT, OP> containingTask,
            StreamConfig.InputConfig[] configuredInputs,
            Map<Integer, StreamConfig> chainedConfigs,
            ClassLoader userCodeClassloader,
            List<StreamOperatorWrapper<?, ?>> allOpWrappers) {
        if (Arrays.stream(configuredInputs)
                .noneMatch(input -> input instanceof StreamConfig.SourceInputConfig)) {
            return Collections.emptyMap();
        }
        checkState(
                mainOperatorWrapper.getStreamOperator() instanceof MultipleInputStreamOperator,
                "Creating chained input is only supported with MultipleInputStreamOperator and MultipleInputStreamTask");
        Map<StreamConfig.SourceInputConfig, ChainedSource> chainedSourceInputs = new HashMap<>();
        MultipleInputStreamOperator<?> multipleInputOperator =
                (MultipleInputStreamOperator<?>) mainOperatorWrapper.getStreamOperator();
        List<Input> operatorInputs = multipleInputOperator.getInputs();

        int sourceInputGateIndex =
                Arrays.stream(containingTask.getEnvironment().getAllInputGates())
                                .mapToInt(IndexedInputGate::getInputGateIndex)
                                .max()
                                .orElse(-1)
                        + 1;

        for (int inputId = 0; inputId < configuredInputs.length; inputId++) {
            if (!(configuredInputs[inputId] instanceof StreamConfig.SourceInputConfig)) {
                continue;
            }
            StreamConfig.SourceInputConfig sourceInput =
                    (StreamConfig.SourceInputConfig) configuredInputs[inputId];
            int sourceEdgeId = sourceInput.getInputEdge().getSourceId();
            StreamConfig sourceInputConfig = chainedConfigs.get(sourceEdgeId);
            OutputTag outputTag = sourceInput.getInputEdge().getOutputTag();

            WatermarkGaugeExposingOutput chainedSourceOutput =
                    createChainedSourceOutput(
                            containingTask,
                            sourceInputConfig,
                            userCodeClassloader,
                            getFinishedOnRestoreInputOrDefault(operatorInputs.get(inputId)),
                            multipleInputOperator.getMetricGroup(),
                            outputTag);

            SourceOperator<?, ?> sourceOperator =
                    (SourceOperator<?, ?>)
                            createOperator(
                                    containingTask,
                                    sourceInputConfig,
                                    userCodeClassloader,
                                    (WatermarkGaugeExposingOutput<StreamRecord<OUT>>)
                                            chainedSourceOutput,
                                    allOpWrappers,
                                    true);
            chainedSourceInputs.put(
                    sourceInput,
                    new ChainedSource(
                            chainedSourceOutput,
                            this.isTaskDeployedAsFinished()
                                    ? new StreamTaskFinishedOnRestoreSourceInput<>(
                                            sourceOperator, sourceInputGateIndex++, inputId)
                                    : new StreamTaskSourceInput<>(
                                            sourceOperator, sourceInputGateIndex++, inputId)));
        }
        return chainedSourceInputs;
    }

    /**
     * Get the numRecordsOut counter for the operator represented by the given config. And re-use
     * the operator-level counter for the task-level numRecordsOut counter if this operator is at
     * the end of the operator chain.
     *
     * <p>Return null if we should not use the numRecordsOut counter to track the records emitted by
     * this operator.
     */
    //获取给定配置表示的运算符的 numRecordsOut 计数器。
    // 如果该运算符位于运算符链的末尾，则将运算符级计数器重新用于任务级 numRecordsOut 计数器。
    //如果我们不应该使用 numRecordsOut 计数器来跟踪此运算符发出的记录，则返回 null。
    @Nullable
    private Counter getOperatorRecordsOutCounter(
            StreamTask<?, ?> containingTask, StreamConfig operatorConfig) {
        ClassLoader userCodeClassloader = containingTask.getUserCodeClassLoader();
        Class<StreamOperatorFactory<?>> streamOperatorFactoryClass =
                operatorConfig.getStreamOperatorFactoryClass(userCodeClassloader);

        // Do not use the numRecordsOut counter on output if this operator is SinkWriterOperator.
        //
        // Metric "numRecordsOut" is defined as the total number of records written to the
        // external system in FLIP-33, but this metric is occupied in AbstractStreamOperator as the
        // number of records sent to downstream operators, which is number of Committable batches
        // sent to SinkCommitter. So we skip registering this metric on output and leave this metric
        // to sink writer implementations to report.
        //如果此运算符是 SinkWriterOperator，请勿在输出上使用 numRecordsOut 计数器。
        // 指标“numRecordsOut”在FLIP-33中被定义为写入外部系统的记录总数，
        // 但该指标在AbstractStreamOperator中被占用为发送到下游算子的记录数，
        // 即发送到SinkCommitter的可提交批次数。
        // 因此，我们跳过在输出上注册此指标，并将此指标留给接收器编写器实现来报告。
        try {
            Class<?> sinkWriterFactoryClass =
                    userCodeClassloader.loadClass(SinkWriterOperatorFactory.class.getName());
            if (sinkWriterFactoryClass.isAssignableFrom(streamOperatorFactoryClass)) {
                return null;
            }
        } catch (ClassNotFoundException e) {
            throw new StreamTaskException(
                    "Could not load SinkWriterOperatorFactory class from userCodeClassloader.", e);
        }

        InternalOperatorMetricGroup operatorMetricGroup =
                containingTask
                        .getEnvironment()
                        .getMetricGroup()
                        .getOrAddOperator(
                                operatorConfig.getOperatorID(), operatorConfig.getOperatorName());

        return operatorMetricGroup.getIOMetricGroup().getNumRecordsOutCounter();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private WatermarkGaugeExposingOutput<StreamRecord> createChainedSourceOutput(
            StreamTask<?, OP> containingTask,
            StreamConfig sourceInputConfig,
            ClassLoader userCodeClassloader,
            Input input,
            OperatorMetricGroup metricGroup,
            OutputTag outputTag) {

        Counter recordsOutCounter = getOperatorRecordsOutCounter(containingTask, sourceInputConfig);

        WatermarkGaugeExposingOutput<StreamRecord> chainedSourceOutput;
        if (containingTask.getExecutionConfig().isObjectReuseEnabled()) {
            chainedSourceOutput =
                    new ChainingOutput(input, recordsOutCounter, metricGroup, outputTag);
        } else {
            TypeSerializer<?> inSerializer =
                    sourceInputConfig.getTypeSerializerOut(userCodeClassloader);
            chainedSourceOutput =
                    new CopyingChainingOutput(
                            input, inSerializer, recordsOutCounter, metricGroup, outputTag);
        }
        /**
         * Chained sources are closed when {@link
         * org.apache.flink.streaming.runtime.io.StreamTaskSourceInput} are being closed.
         */
        return closer.register(chainedSourceOutput);
    }

    //在createOutputCollector(...)中会调用createOperatorChain(...)方法，
    //而createOperatorChain(...)会递归调用createOutputCollector(...)方法，
    //目的是以从后往前的形式一个一个构造算子链中的算子实例。
    //在构造算子实例的过程中，都会设置该算子的output信息，output信息包含下一个算子的引用，
    //末尾的operator的output就是RecordWriterOutput。
    private <T> WatermarkGaugeExposingOutput<StreamRecord<T>> createOutputCollector(
            StreamTask<?, ?> containingTask,
            StreamConfig operatorConfig,
            Map<Integer, StreamConfig> chainedConfigs,
            ClassLoader userCodeClassloader,
            Map<IntermediateDataSetID, RecordWriterOutput<?>> recordWriterOutputs,
            List<StreamOperatorWrapper<?, ?>> allOperatorWrappers,
            MailboxExecutorFactory mailboxExecutorFactory,
            boolean shouldAddMetric) {
        List<OutputWithChainingCheck<StreamRecord<T>>> allOutputs = new ArrayList<>(4);

        // create collectors for the network outputs
        //为网络输出创建收集器
        for (NonChainedOutput streamOutput :
                operatorConfig.getOperatorNonChainedOutputs(userCodeClassloader)) {
            @SuppressWarnings("unchecked")
            RecordWriterOutput<T> recordWriterOutput =
                    (RecordWriterOutput<T>) recordWriterOutputs.get(streamOutput.getDataSetId());

            allOutputs.add(recordWriterOutput);
        }

        // Create collectors for the chained outputs
        //为链式输出创建收集器
        for (StreamEdge outputEdge : operatorConfig.getChainedOutputs(userCodeClassloader)) {
            int outputId = outputEdge.getTargetId();
            StreamConfig chainedOpConfig = chainedConfigs.get(outputId);

            WatermarkGaugeExposingOutput<StreamRecord<T>> output =
                    //递归地创建, 运算符是尾对头创建的
                    createOperatorChain(
                            containingTask,
                            operatorConfig,
                            chainedOpConfig,
                            chainedConfigs,
                            userCodeClassloader,
                            recordWriterOutputs,
                            allOperatorWrappers,
                            outputEdge.getOutputTag(),
                            mailboxExecutorFactory,
                            shouldAddMetric);
            checkState(output instanceof OutputWithChainingCheck);
            allOutputs.add((OutputWithChainingCheck) output);
            // If the operator has multiple downstream chained operators, only one of them should
            // increment the recordsOutCounter for this operator. Set shouldAddMetric to false
            // so that we would skip adding the counter to other downstream operators.
            //如果运算符具有多个下游链接的运算符，则其中只有一个应递增此运算符的recordsOutCounter。
            //将shouldAddMetric设置为false，以便我们跳过将计数器添加到其他下游运算符。
            shouldAddMetric = false;
        }

        WatermarkGaugeExposingOutput<StreamRecord<T>> result;

        if (allOutputs.size() == 1) {
            result = allOutputs.get(0);
            // only if this is a single RecordWriterOutput, reuse its numRecordOut for task.
            //仅当这是单个RecordWriterOutput时，才对任务重用其numRecordOut。
            if (result instanceof RecordWriterOutput) {
                Counter numRecordsOutCounter = createNumRecordsOutCounter(containingTask);
                ((RecordWriterOutput<T>) result).setNumRecordsOut(numRecordsOutCounter);
            }
        } else {
            // send to N outputs. Note that this includes the special case
            // of sending to zero outputs
            //发送到N个输出。请注意，这包括发送到零输出的特殊情况
            @SuppressWarnings({"unchecked"})
            OutputWithChainingCheck<StreamRecord<T>>[] allOutputsArray =
                    new OutputWithChainingCheck[allOutputs.size()];
            for (int i = 0; i < allOutputs.size(); i++) {
                allOutputsArray[i] = allOutputs.get(i);
            }

            // This is the inverse of creating the normal ChainingOutput.
            // If the chaining output does not copy we need to copy in the broadcast output,
            // otherwise multi-chaining would not work correctly.
            //这与创建正常的ChainingOutput相反。如果链接输出不复制，
            //我们需要在广播输出中复制，否则多链接将无法正常工作。
            Counter numRecordsOutForTask = createNumRecordsOutCounter(containingTask);
            if (containingTask.getExecutionConfig().isObjectReuseEnabled()) {
                result =
                        closer.register(
                                new CopyingBroadcastingOutputCollector<>(
                                        allOutputsArray, numRecordsOutForTask));
            } else {
                result =
                        closer.register(
                                new BroadcastingOutputCollector<>(
                                        allOutputsArray, numRecordsOutForTask));
            }
        }

        if (shouldAddMetric) {
            // Create a CountingOutput to increment the recordsOutCounter for this operator
            // if we have not added the counter to any downstream chained operator.
            //如果我们尚未将计数器添加到任何下游链接运算符，则创建CountingOutput以递增此运算符的recordsOutCounter。
            Counter recordsOutCounter =
                    getOperatorRecordsOutCounter(containingTask, operatorConfig);
            if (recordsOutCounter != null) {
                result = new CountingOutput<>(result, recordsOutCounter);
            }
        }
        return result;
    }

    private static Counter createNumRecordsOutCounter(StreamTask<?, ?> containingTask) {
        TaskIOMetricGroup taskIOMetricGroup =
                containingTask.getEnvironment().getMetricGroup().getIOMetricGroup();
        Counter counter = new SimpleCounter();
        taskIOMetricGroup.reuseRecordsOutputCounter(counter);
        return counter;
    }

    /**
     * Recursively create chain of operators that starts from the given {@param operatorConfig}.
     * Operators are created tail to head and wrapped into an {@link WatermarkGaugeExposingOutput}.
     */
    //递归地创建从给定的 @ param operatorConfig开始的运算符链。
    // 运算符是尾对头创建的，并包装到WatermarkGaugeExposingOutput中。
    private <IN, OUT> WatermarkGaugeExposingOutput<StreamRecord<IN>> createOperatorChain(
            StreamTask<OUT, ?> containingTask,
            StreamConfig prevOperatorConfig,
            StreamConfig operatorConfig,
            Map<Integer, StreamConfig> chainedConfigs,
            ClassLoader userCodeClassloader,
            Map<IntermediateDataSetID, RecordWriterOutput<?>> recordWriterOutputs,
            List<StreamOperatorWrapper<?, ?>> allOperatorWrappers,
            OutputTag<IN> outputTag,
            MailboxExecutorFactory mailboxExecutorFactory,
            boolean shouldAddMetricForPrevOperator) {
        // create the output that the operator writes to first. this may recursively create more
        // operators
        //创建运算符首先写入的输出。这可能会递归地创建更多的运算符
        WatermarkGaugeExposingOutput<StreamRecord<OUT>> chainedOperatorOutput =
                createOutputCollector(
                        containingTask,
                        operatorConfig,
                        chainedConfigs,
                        userCodeClassloader,
                        recordWriterOutputs,
                        allOperatorWrappers,
                        mailboxExecutorFactory,
                        true);

        OneInputStreamOperator<IN, OUT> chainedOperator =
                createOperator(
                        containingTask,
                        operatorConfig,
                        userCodeClassloader,
                        chainedOperatorOutput,
                        allOperatorWrappers,
                        false);

        return wrapOperatorIntoOutput(
                chainedOperator,
                containingTask,
                prevOperatorConfig,
                operatorConfig,
                userCodeClassloader,
                outputTag,
                shouldAddMetricForPrevOperator);
    }

    /**
     * Create and return a single operator from the given {@param operatorConfig} that will be
     * producing records to the {@param output}.
     */
    //从给定的 @ param operatorConfig创建并返回单个运算符，该运算符将生成记录到 @ param输出
    private <OUT, OP extends StreamOperator<OUT>> OP createOperator(
            StreamTask<OUT, ?> containingTask,
            StreamConfig operatorConfig,
            ClassLoader userCodeClassloader,
            WatermarkGaugeExposingOutput<StreamRecord<OUT>> output,
            List<StreamOperatorWrapper<?, ?>> allOperatorWrappers,
            boolean isHead) {

        // now create the operator and give it the output collector to write its output to
        //现在创建运算符并为其提供输出收集器以将其输出写入
        Tuple2<OP, Optional<ProcessingTimeService>> chainedOperatorAndTimeService =
                StreamOperatorFactoryUtil.createOperator(
                        operatorConfig.getStreamOperatorFactory(userCodeClassloader),
                        containingTask,
                        operatorConfig,
                        output,
                        operatorEventDispatcher);

        OP chainedOperator = chainedOperatorAndTimeService.f0;
        allOperatorWrappers.add(
                createOperatorWrapper(
                        chainedOperator,
                        containingTask,
                        operatorConfig,
                        chainedOperatorAndTimeService.f1,
                        isHead));

        chainedOperator
                .getMetricGroup()
                .gauge(
                        MetricNames.IO_CURRENT_OUTPUT_WATERMARK,
                        output.getWatermarkGauge()::getValue);
        return chainedOperator;
    }

    private <IN, OUT> WatermarkGaugeExposingOutput<StreamRecord<IN>> wrapOperatorIntoOutput(
            OneInputStreamOperator<IN, OUT> operator,
            StreamTask<OUT, ?> containingTask,
            StreamConfig prevOperatorConfig,
            StreamConfig operatorConfig,
            ClassLoader userCodeClassloader,
            OutputTag<IN> outputTag,
            boolean shouldAddMetricForPrevOperator) {

        Counter recordsOutCounter = null;

        if (shouldAddMetricForPrevOperator) {
            recordsOutCounter = getOperatorRecordsOutCounter(containingTask, prevOperatorConfig);
        }

        WatermarkGaugeExposingOutput<StreamRecord<IN>> currentOperatorOutput;
        if (containingTask.getExecutionConfig().isObjectReuseEnabled()) {
            currentOperatorOutput =
                    new ChainingOutput<>(
                            operator, recordsOutCounter, operator.getMetricGroup(), outputTag);
        } else {
            TypeSerializer<IN> inSerializer =
                    operatorConfig.getTypeSerializerIn1(userCodeClassloader);
            currentOperatorOutput =
                    new CopyingChainingOutput<>(
                            operator,
                            inSerializer,
                            recordsOutCounter,
                            operator.getMetricGroup(),
                            outputTag);
        }

        // wrap watermark gauges since registered metrics must be unique
        //包装水印仪表，因为注册的指标必须是唯一的
        operator.getMetricGroup()
                .gauge(
                        MetricNames.IO_CURRENT_INPUT_WATERMARK,
                        currentOperatorOutput.getWatermarkGauge()::getValue);

        return closer.register(currentOperatorOutput);
    }

    /**
     * Links operator wrappers in forward topological order.
     *
     * @param allOperatorWrappers is an operator wrapper list of reverse topological order
     */
    //按正向拓扑顺序链接运算符包装器。
    private StreamOperatorWrapper<?, ?> linkOperatorWrappers(
            List<StreamOperatorWrapper<?, ?>> allOperatorWrappers) {
        StreamOperatorWrapper<?, ?> previous = null;
        for (StreamOperatorWrapper<?, ?> current : allOperatorWrappers) {
            if (previous != null) {
                previous.setPrevious(current);
            }
            current.setNext(previous);
            previous = current;
        }
        return previous;
    }

    private <T, P extends StreamOperator<T>> StreamOperatorWrapper<T, P> createOperatorWrapper(
            P operator,
            StreamTask<?, ?> containingTask,
            StreamConfig operatorConfig,
            Optional<ProcessingTimeService> processingTimeService,
            boolean isHead) {
        return new StreamOperatorWrapper<>(
                operator,
                processingTimeService,
                containingTask
                        .getMailboxExecutorFactory()
                        .createExecutor(operatorConfig.getChainIndex()),
                isHead);
    }

    protected void sendAcknowledgeCheckpointEvent(long checkpointId) {
        if (operatorEventDispatcher == null) {
            return;
        }

        operatorEventDispatcher
                .getRegisteredOperators()
                .forEach(
                        x ->
                                operatorEventDispatcher
                                        .getOperatorEventGateway(x)
                                        .sendEventToCoordinator(
                                                new AcknowledgeCheckpointEvent(checkpointId)));
    }
}
