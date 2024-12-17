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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.partition.consumer.CheckpointableInput;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.operators.SourceOperator;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Implementation of {@link StreamTaskInput} that reads data from the {@link SourceOperator} and
 * returns the {@link DataInputStatus} to indicate whether the source state is available,
 * unavailable or finished.
 */
//StreamTaskInput的实现，从SourceOperator读取数据并返回DataInputStatus以指示源状态是否可用、不可用或已完成
@Internal
public class StreamTaskSourceInput<T> implements StreamTaskInput<T>, CheckpointableInput {

    private final SourceOperator<T, ?> operator;
    private final int inputGateIndex;
    private final AvailabilityHelper isBlockedAvailability = new AvailabilityHelper();
    private final List<InputChannelInfo> inputChannelInfos;
    private final int inputIndex;

    public StreamTaskSourceInput(
            SourceOperator<T, ?> operator, int inputGateIndex, int inputIndex) {
        this.operator = checkNotNull(operator);
        this.inputGateIndex = inputGateIndex;
        inputChannelInfos = Collections.singletonList(new InputChannelInfo(inputGateIndex, 0));
        isBlockedAvailability.resetAvailable();
        this.inputIndex = inputIndex;
    }

    @Override
    public DataInputStatus emitNext(DataOutput<T> output) throws Exception {
        /**
         * Safe guard against best efforts availability checks. If despite being unavailable someone
         * polls the data from this source while it's blocked, it should return {@link
         * DataInputStatus.NOTHING_AVAILABLE}.
         */
        //安全防范尽力而为的可用性检查。
        // 如果尽管不可用，但有人在该源被阻止时轮询该源的数据，则它应该返回 {@link DataInputStatus.NOTHING_AVAILABLE}。
        if (isBlockedAvailability.isApproximatelyAvailable()) {
            return operator.emitNext(output);
        }
        return DataInputStatus.NOTHING_AVAILABLE;
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        return isBlockedAvailability.and(operator);
    }

    @Override
    public void blockConsumption(InputChannelInfo channelInfo) {
        isBlockedAvailability.resetUnavailable();
    }

    @Override
    public void resumeConsumption(InputChannelInfo channelInfo) {
        isBlockedAvailability.getUnavailableToResetAvailable().complete(null);
    }

    @Override
    public List<InputChannelInfo> getChannelInfos() {
        return inputChannelInfos;
    }

    @Override
    public int getNumberOfInputChannels() {
        return inputChannelInfos.size();
    }

    /**
     * This method is used with unaligned checkpoints to mark the arrival of a first {@link
     * CheckpointBarrier}. For chained sources, there is no {@link CheckpointBarrier} per se flowing
     * through the job graph. We can assume that an imaginary {@link CheckpointBarrier} was produced
     * by the source, at any point of time of our choosing.
     *
     * <p>We are choosing to interpret it, that {@link CheckpointBarrier} for sources was received
     * immediately as soon as we receive either checkpoint start RPC, or {@link CheckpointBarrier}
     * from a network input. So that we can checkpoint state of the source and all of the other
     * operators at the same time.
     *
     * <p>Also we are choosing to block the source, as a best effort optimisation as: - either there
     * is no backpressure and the checkpoint "alignment" will happen very quickly anyway - or there
     * is a backpressure, and it's better to prioritize processing data from the network to speed up
     * checkpointing. From the cluster resource utilisation perspective, by blocking chained source
     * doesn't block any resources from being used, as this task running the source has a backlog of
     * buffered input data waiting to be processed.
     *
     * <p>However from the correctness point of view, {@link #checkpointStarted(CheckpointBarrier)}
     * and {@link #checkpointStopped(long)} methods could be empty no-op.
     */
    //此方法与未对齐的检查点一起使用来标记第一个CheckpointBarrier的到达。
    // 对于链式源，本身没有CheckpointBarrier流经作业图。
    // 我们可以假设一个虚构的CheckpointBarrier是由源在我们选择的任何时间点生成的。
    //我们选择解释它，一旦我们收到检查点启动 RPC 或来自网络输入的CheckpointBarrier就会立即收到源的CheckpointBarrier 。
    // 这样我们就可以同时检查源和所有其他运算符的状态。
    //此外，我们选择阻止源头，作为尽最大努力的优化：
    // - 要么没有背压，检查点“对齐”无论如何都会很快发生
    // - 或者有背压，最好优先处理来自网络的数据以加快检查点速度。
    // 从集群资源利用的角度来看，阻止链式源不会阻止任何资源的使用，因为运行源的此任务有大量等待处理的缓冲输入数据。
    //然而，从正确性的角度来看，checkpointStarted(CheckpointBarrier)和checkpointStopped(long)方法可能是空的无操作。
    @Override
    public void checkpointStarted(CheckpointBarrier barrier) {
        blockConsumption(null);
    }

    @Override
    public void checkpointStopped(long cancelledCheckpointId) {
        resumeConsumption(null);
    }

    @Override
    public int getInputGateIndex() {
        return inputGateIndex;
    }

    @Override
    public void convertToPriorityEvent(int channelIndex, int sequenceNumber) throws IOException {}

    @Override
    public int getInputIndex() {
        return inputIndex;
    }

    @Override
    public void close() {
        // SourceOperator is closed via OperatorChain
        // SourceOperator 通过 OperatorChain 关闭
    }

    @Override
    public CompletableFuture<Void> prepareSnapshot(
            ChannelStateWriter channelStateWriter, long checkpointId) throws CheckpointException {
        return CompletableFuture.completedFuture(null);
    }

    public OperatorID getOperatorID() {
        return operator.getOperatorID();
    }

    public SourceOperator<T, ?> getOperator() {
        return operator;
    }
}
