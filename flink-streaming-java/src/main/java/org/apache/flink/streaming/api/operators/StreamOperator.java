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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.Serializable;

/**
 * Basic interface for stream operators. Implementers would implement one of {@link
 * org.apache.flink.streaming.api.operators.OneInputStreamOperator} or {@link
 * org.apache.flink.streaming.api.operators.TwoInputStreamOperator} to create operators that process
 * elements.
 *
 * <p>The class {@link org.apache.flink.streaming.api.operators.AbstractStreamOperator} offers
 * default implementation for the lifecycle and properties methods.
 *
 * <p>Methods of {@code StreamOperator} are guaranteed not to be called concurrently. Also, if using
 * the timer service, timer callbacks are also guaranteed not to be called concurrently with methods
 * on {@code StreamOperator}.
 *
 * @param <OUT> The output type of the operator
 */
//流操作符的基本接口。实现者将实现OneInputStreamOperator或TwoInputStreamOperator之一来创建处理元素的运算符。
//AbstractStreamOperator类提供生命周期和属性方法的默认实现。
//StreamOperator的方法保证不会被并发调用。
// 此外，如果使用计时器服务，还保证计时器回调不会与StreamOperator上的方法同时调用。
@PublicEvolving
public interface StreamOperator<OUT> extends CheckpointListener, KeyContext, Serializable {

    // ------------------------------------------------------------------------
    //  life cycle
    // ------------------------------------------------------------------------

    /**
     * This method is called immediately before any elements are processed, it should contain the
     * operator's initialization logic.
     *
     * @implSpec In case of recovery, this method needs to ensure that all recovered data is
     *     processed before passing back control, so that the order of elements is ensured during
     *     the recovery of an operator chain (operators are opened from the tail operator to the
     *     head operator).
     * @throws java.lang.Exception An exception in this method causes the operator to fail.
     */
    //在处理任何元素之前立即调用此方法，它应包含运算符的初始化逻辑。
    void open() throws Exception;

    /**
     * This method is called at the end of data processing.
     *
     * <p>The method is expected to flush all remaining buffered data. Exceptions during this
     * flushing of buffered data should be propagated, in order to cause the operation to be
     * recognized as failed, because the last data items are not processed properly.
     *
     * <p><b>After this method is called, no more records can be produced for the downstream
     * operators.</b>
     *
     * <p><b>WARNING:</b> It is not safe to use this method to commit any transactions or other side
     * effects! You can use this method to flush any buffered data that can later on be committed
     * e.g. in a {@link StreamOperator#notifyCheckpointComplete(long)}.
     *
     * <p><b>NOTE:</b>This method does not need to close any resources. You should release external
     * resources in the {@link #close()} method.
     *
     * @throws java.lang.Exception An exception in this method causes the operator to fail.
     */
    //该方法在数据处理结束时调用。
    //该方法预计会刷新所有剩余的缓冲数据。
    // 应传播缓冲数据刷新期间的异常，以便导致操作被识别为失败，因为最后的数据项未正确处理。
    //调用该方法后，下游算子将无法再产生记录。
    //警告：使用此方法提交任何事务或其他副作用是不安全的！
    // 您可以使用此方法刷新任何稍后可以提交的缓冲数据，例如在notifyCheckpointComplete(long)中。
    //注意：此方法不需要关闭任何资源。您应该在close()方法中释放外部资源。
    void finish() throws Exception;

    /**
     * This method is called at the very end of the operator's life, both in the case of a
     * successful completion of the operation, and in the case of a failure and canceling.
     *
     * <p>This method is expected to make a thorough effort to release all resources that the
     * operator has acquired.
     *
     * <p><b>NOTE:</b>It can not emit any records! If you need to emit records at the end of
     * processing, do so in the {@link #finish()} method.
     */
    //该方法在操作符生命周期的最后阶段被调用，无论是在操作成功完成的情况下，还是在操作失败和取消的情况下。
    //该方法有望彻底释放运营商已获取的所有资源。
    //注意：它不能发出任何记录！如果需要在处理结束时发出记录，请在finish()方法中执行此操作
    void close() throws Exception;

    // ------------------------------------------------------------------------
    //  state snapshots
    // ------------------------------------------------------------------------

    /**
     * This method is called when the operator should do a snapshot, before it emits its own
     * checkpoint barrier.
     *
     * <p>This method is intended not for any actual state persistence, but only for emitting some
     * data before emitting the checkpoint barrier. Operators that maintain some small transient
     * state that is inefficient to checkpoint (especially when it would need to be checkpointed in
     * a re-scalable way) but can simply be sent downstream before the checkpoint. An example are
     * opportunistic pre-aggregation operators, which have small the pre-aggregation state that is
     * frequently flushed downstream.
     *
     * <p><b>Important:</b> This method should not be used for any actual state snapshot logic,
     * because it will inherently be within the synchronous part of the operator's checkpoint. If
     * heavy work is done within this method, it will affect latency and downstream checkpoint
     * alignments.
     *
     * @param checkpointId The ID of the checkpoint.
     * @throws Exception Throwing an exception here causes the operator to fail and go into
     *     recovery.
     */
    //当操作员应该在发出自己的检查点屏障之前执行快照时调用此方法。
    //此方法并非用于任何实际状态持久性，而仅用于在发出检查点屏障之前发出一些数据。
    // 维护一些小的瞬态状态的操作符对于检查点来说效率低下（特别是当需要以可重新扩展的方式进行检查点时），
    // 但可以简单地在检查点之前发送到下游。一个例子是机会预聚合算子，它们的预聚合状态很小，经常被下游刷新。
    //重要提示：此方法不应用于任何实际状态快照逻辑，因为它本质上位于操作员检查点的同步部分内。
    // 如果在此方法中完成繁重的工作，它将影响延迟和下游检查点对齐。
    void prepareSnapshotPreBarrier(long checkpointId) throws Exception;

    /**
     * Called to draw a state snapshot from the operator.
     *
     * @return a runnable future to the state handle that points to the snapshotted state. For
     *     synchronous implementations, the runnable might already be finished.
     * @throws Exception exception that happened during snapshotting.
     */
    //调用以从运算符绘制状态快照。
    OperatorSnapshotFutures snapshotState(
            long checkpointId,
            long timestamp,
            CheckpointOptions checkpointOptions,
            CheckpointStreamFactory storageLocation)
            throws Exception;

    /** Provides a context to initialize all state in the operator. */
   //提供上下文以初始化运算符中的所有状态。
    void initializeState(StreamTaskStateInitializer streamTaskStateManager) throws Exception;

    // ------------------------------------------------------------------------
    //  miscellaneous
    // ------------------------------------------------------------------------

    void setKeyContextElement1(StreamRecord<?> record) throws Exception;

    void setKeyContextElement2(StreamRecord<?> record) throws Exception;

    OperatorMetricGroup getMetricGroup();

    OperatorID getOperatorID();

    /**
     * Called to get the OperatorAttributes of the operator. If there is no defined attribute, a
     * default OperatorAttributes is built.
     *
     * @return OperatorAttributes of the operator.
     */
    //调用以获取操作员的 OperatorAttributes。如果没有定义的属性，则会构建默认的 OperatorAttributes
    @Experimental
    default OperatorAttributes getOperatorAttributes() {
        return new OperatorAttributesBuilder().build();
    }
}
