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

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.state.InputChannelStateHandle;
import org.apache.flink.runtime.state.ResultSubpartitionStateHandle;
import org.apache.flink.util.CloseableIterator;

import java.io.Closeable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** Writes channel state during checkpoint/savepoint. */
//在检查点/ 保存点期间写入通道状态
@Internal
public interface ChannelStateWriter extends Closeable {

    /** Channel state write result. */
    //通道状态写入结果。
    class ChannelStateWriteResult {
        final CompletableFuture<Collection<InputChannelStateHandle>> inputChannelStateHandles;
        final CompletableFuture<Collection<ResultSubpartitionStateHandle>>
                resultSubpartitionStateHandles;

        ChannelStateWriteResult() {
            this(new CompletableFuture<>(), new CompletableFuture<>());
        }

        ChannelStateWriteResult(
                CompletableFuture<Collection<InputChannelStateHandle>> inputChannelStateHandles,
                CompletableFuture<Collection<ResultSubpartitionStateHandle>>
                        resultSubpartitionStateHandles) {
            this.inputChannelStateHandles = inputChannelStateHandles;
            this.resultSubpartitionStateHandles = resultSubpartitionStateHandles;
        }

        public CompletableFuture<Collection<InputChannelStateHandle>>
                getInputChannelStateHandles() {
            return inputChannelStateHandles;
        }

        public CompletableFuture<Collection<ResultSubpartitionStateHandle>>
                getResultSubpartitionStateHandles() {
            return resultSubpartitionStateHandles;
        }

        public static final ChannelStateWriteResult EMPTY =
                new ChannelStateWriteResult(
                        CompletableFuture.completedFuture(Collections.emptyList()),
                        CompletableFuture.completedFuture(Collections.emptyList()));

        public void fail(Throwable e) {
            inputChannelStateHandles.completeExceptionally(e);
            resultSubpartitionStateHandles.completeExceptionally(e);
        }

        public boolean isDone() {
            return inputChannelStateHandles.isDone() && resultSubpartitionStateHandles.isDone();
        }

        @VisibleForTesting
        public void waitForDone() {
            try {
                inputChannelStateHandles.get();
            } catch (Throwable ignored) {
            }
            try {
                resultSubpartitionStateHandles.get();
            } catch (Throwable ignored) {
            }
        }
    }

    /**
     * Sequence number for the buffers that were saved during the previous execution attempt; then
     * restored; and now are to be saved again (as opposed to the buffers received from the upstream
     * or from the operator).
     */
    //上次执行尝试期间保存的缓冲区的序列号；然后恢复；现在将再次保存（与从上游或操作员接收的缓冲区相反）。
    int SEQUENCE_NUMBER_RESTORED = -1;

    /**
     * Signifies that buffer sequence number is unknown (e.g. if passing sequence numbers is not
     * implemented).
     */
    //表示缓冲区序列号未知（例如，如果未实现传递序列号）。
    int SEQUENCE_NUMBER_UNKNOWN = -2;

    /** Initiate write of channel state for the given checkpoint id. */
    //启动给定检查点id的通道状态写入。
    void start(long checkpointId, CheckpointOptions checkpointOptions);

    /**
     * Add in-flight buffers from the {@link
     * org.apache.flink.runtime.io.network.partition.consumer.InputChannel InputChannel}. Must be
     * called after {@link #start(long,CheckpointOptions)} and before {@link #finishInput(long)}.
     * Buffers are recycled after they are written or exception occurs.
     *
     * @param startSeqNum sequence number of the 1st passed buffer. It is intended to use for
     *     incremental snapshots. If no data is passed it is ignored.
     * @param data zero or more <b>data</b> buffers ordered by their sequence numbers
     * @see org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter#SEQUENCE_NUMBER_RESTORED
     * @see org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter#SEQUENCE_NUMBER_UNKNOWN
     */
    //从InputChannel添加运行中缓冲区。
    // 必须在start(long, CheckpointOptions)之后和finishInput(long)之前调用。
    // 缓冲区在写入或发生异常后被回收。
    //参数：
    //startSeqNum – 第一个传递的缓冲区的序列号。它旨在用于增量快照。如果没有数据被传递，它将被忽略。
    //data – 零个或多个数据缓冲区按其序列号排序
    void addInputData(
            long checkpointId,
            InputChannelInfo info,
            int startSeqNum,
            CloseableIterator<Buffer> data);

    /**
     * Add in-flight buffers from the {@link
     * org.apache.flink.runtime.io.network.partition.ResultSubpartition ResultSubpartition}. Must be
     * called after {@link #start} and before {@link #finishOutput(long)}. Buffers are recycled
     * after they are written or exception occurs.
     *
     * @param startSeqNum sequence number of the 1st passed buffer. It is intended to use for
     *     incremental snapshots. If no data is passed it is ignored.
     * @param data zero or more <b>data</b> buffers ordered by their sequence numbers
     * @throws IllegalArgumentException if one or more passed buffers {@link Buffer#isBuffer() isn't
     *     a buffer}
     * @see org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter#SEQUENCE_NUMBER_RESTORED
     * @see org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter#SEQUENCE_NUMBER_UNKNOWN
     */
    //从ResultSubpartition添加运行中的缓冲区。
    // 必须在start之后和finishOutput(long)之前调用。
    // 缓冲区在写入或发生异常后被回收。
    //参数：
    //startSeqNum – 第一个传递的缓冲区的序列号。它旨在用于增量快照。如果没有数据被传递，它将被忽略。
    // data – 零个或多个数据缓冲区按其序列号排序
    void addOutputData(
            long checkpointId, ResultSubpartitionInfo info, int startSeqNum, Buffer... data)
            throws IllegalArgumentException;

    /**
     * Add in-flight bufferFuture from the {@link
     * org.apache.flink.runtime.io.network.partition.ResultSubpartition ResultSubpartition}. Must be
     * called after {@link #start} and before {@link #finishOutput(long)}. Buffers are recycled
     * after they are written or exception occurs.
     *
     * <p>The method will be called when the unaligned checkpoint is enabled and received an aligned
     * barrier.
     */
    //从ResultSubpartition添加运行中的 bufferFuture 。
    // 必须在start之后和finishOutput(long)之前调用。缓冲区在写入或发生异常后被回收。
    //当启用未对齐检查点并收到对齐屏障时，将调用该方法。
    void addOutputDataFuture(
            long checkpointId,
            ResultSubpartitionInfo info,
            int startSeqNum,
            CompletableFuture<List<Buffer>> data)
            throws IllegalArgumentException;

    /**
     * Finalize write of channel state data for the given checkpoint id. Must be called after {@link
     * #start(long, CheckpointOptions)} and all of the input data of the given checkpoint added.
     * When both {@link #finishInput} and {@link #finishOutput} were called the results can be
     * (eventually) obtained using {@link #getAndRemoveWriteResult}
     */
    //完成给定检查点 ID 的通道状态数据的写入。
    // 必须在start(long, CheckpointOptions)并添加给定检查点的所有输入数据之后调用。
    // 当同时调用finishInput和finishOutput时，可以（最终）使用getAndRemoveWriteResult获得结果
    void finishInput(long checkpointId);

    /**
     * Finalize write of channel state data for the given checkpoint id. Must be called after {@link
     * #start(long, CheckpointOptions)} and all of the output data of the given checkpoint added.
     * When both {@link #finishInput} and {@link #finishOutput} were called the results can be
     * (eventually) obtained using {@link #getAndRemoveWriteResult}
     */
    //完成给定检查点id的通道状态数据的写入。必须在start(long，CheckpointOptions) 之后调用，并添加给定检查点的所有输出数据。
    //当同时调用finishInput和finishOutput时，可以 (最终) 使用getandremovwriteresult获得结果
    void finishOutput(long checkpointId);

    /**
     * Aborts the checkpoint and fails pending result for this checkpoint.
     *
     * @param cleanup true if {@link #getAndRemoveWriteResult(long)} is not supposed to be called
     *     afterwards.
     */
    //中止检查点并使该检查点的待处理结果失败。
    void abort(long checkpointId, Throwable cause, boolean cleanup);

    /**
     * Must be called after {@link #start(long, CheckpointOptions)} once.
     *
     * @throws IllegalArgumentException if the passed checkpointId is not known.
     */
    //必须在start(long, CheckpointOptions)后调用一次。
    ChannelStateWriteResult getAndRemoveWriteResult(long checkpointId)
            throws IllegalArgumentException;

    ChannelStateWriter NO_OP = new NoOpChannelStateWriter();

    /** No-op implementation of {@link ChannelStateWriter}. */
    //ChannelStateWriter的无操作实现。
    class NoOpChannelStateWriter implements ChannelStateWriter {
        @Override
        public void start(long checkpointId, CheckpointOptions checkpointOptions) {}

        @Override
        public void addInputData(
                long checkpointId,
                InputChannelInfo info,
                int startSeqNum,
                CloseableIterator<Buffer> data) {}

        @Override
        public void addOutputData(
                long checkpointId, ResultSubpartitionInfo info, int startSeqNum, Buffer... data) {}

        @Override
        public void addOutputDataFuture(
                long checkpointId,
                ResultSubpartitionInfo info,
                int startSeqNum,
                CompletableFuture<List<Buffer>> data) {}

        @Override
        public void finishInput(long checkpointId) {}

        @Override
        public void finishOutput(long checkpointId) {}

        @Override
        public void abort(long checkpointId, Throwable cause, boolean cleanup) {}

        @Override
        public ChannelStateWriteResult getAndRemoveWriteResult(long checkpointId) {
            return new ChannelStateWriteResult(
                    CompletableFuture.completedFuture(Collections.emptyList()),
                    CompletableFuture.completedFuture(Collections.emptyList()));
        }

        @Override
        public void close() {}
    }
}
