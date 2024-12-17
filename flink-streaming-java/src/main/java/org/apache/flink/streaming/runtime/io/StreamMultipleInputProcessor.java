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
import org.apache.flink.streaming.api.operators.InputSelection;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.util.ExceptionUtils;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/** Input processor for {@link MultipleInputStreamOperator}. */
//MultipleInputStreamOperator的输入处理器。
@Internal
public final class StreamMultipleInputProcessor implements StreamInputProcessor {

    private final MultipleInputSelectionHandler inputSelectionHandler;

    private final StreamOneInputProcessor<?>[] inputProcessors;

    private final MultipleFuturesAvailabilityHelper availabilityHelper;
    /** Always try to read from the first input. */
    //始终尝试从第一个输入开始读取。
    private int lastReadInputIndex = 1;

    private boolean isPrepared;

    public StreamMultipleInputProcessor(
            MultipleInputSelectionHandler inputSelectionHandler,
            StreamOneInputProcessor<?>[] inputProcessors) {
        this.inputSelectionHandler = inputSelectionHandler;
        this.inputProcessors = inputProcessors;
        this.availabilityHelper = new MultipleFuturesAvailabilityHelper(inputProcessors.length);
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        if (inputSelectionHandler.isAnyInputAvailable()
                || inputSelectionHandler.areAllInputsFinished()) {
            return AVAILABLE;
        }

        availabilityHelper.resetToUnAvailable();
        for (int i = 0; i < inputProcessors.length; i++) {
            if (!inputSelectionHandler.isInputFinished(i)
                    && inputSelectionHandler.isInputSelected(i)) {
                availabilityHelper.anyOf(i, inputProcessors[i].getAvailableFuture());
            }
        }
        return availabilityHelper.getAvailableFuture();
    }

    @Override
    public DataInputStatus processInput() throws Exception {
        int readingInputIndex;
        if (isPrepared) {
            readingInputIndex = selectNextReadingInputIndex();
        } else {
            // the preparations here are not placed in the constructor because all work in it
            // must be executed after all operators are opened.
            //这里的准备工作没有放在构造函数中，因为里面的所有工作都必须在所有操作符打开之后执行。
            readingInputIndex = selectFirstReadingInputIndex();
        }
        if (readingInputIndex == InputSelection.NONE_AVAILABLE) {
            return DataInputStatus.NOTHING_AVAILABLE;
        }

        lastReadInputIndex = readingInputIndex;
        DataInputStatus inputStatus = inputProcessors[readingInputIndex].processInput();
        return inputSelectionHandler.updateStatusAndSelection(inputStatus, readingInputIndex);
    }

    private int selectFirstReadingInputIndex() {
        // Note: the first call to nextSelection () on the operator must be made after this operator
        // is opened to ensure that any changes about the input selection in its open()
        // method take effect.
        //注意：操作符上的 nextSelection() 的第一次调用必须在该操作符打开后进行，
        // 以确保其 open() 方法中有关输入选择的任何更改生效。
        inputSelectionHandler.nextSelection();

        isPrepared = true;

        return selectNextReadingInputIndex();
    }

    @Override
    public void close() throws IOException {
        IOException ex = null;
        for (StreamOneInputProcessor<?> input : inputProcessors) {
            try {
                input.close();
            } catch (IOException e) {
                ex = ExceptionUtils.firstOrSuppressed(e, ex);
            }
        }

        if (ex != null) {
            throw ex;
        }
    }

    @Override
    public CompletableFuture<Void> prepareSnapshot(
            ChannelStateWriter channelStateWriter, long checkpointId) throws CheckpointException {
        CompletableFuture<?>[] inputFutures = new CompletableFuture[inputProcessors.length];
        for (int index = 0; index < inputFutures.length; index++) {
            inputFutures[index] =
                    inputProcessors[index].prepareSnapshot(channelStateWriter, checkpointId);
        }
        return CompletableFuture.allOf(inputFutures);
    }

    private int selectNextReadingInputIndex() {
        if (!inputSelectionHandler.isAnyInputAvailable()) {
            fullCheckAndSetAvailable();
        }

        int readingInputIndex = inputSelectionHandler.selectNextInputIndex(lastReadInputIndex);
        if (readingInputIndex == InputSelection.NONE_AVAILABLE) {
            return InputSelection.NONE_AVAILABLE;
        }

        // to avoid starvation, if the input selection is ALL and availableInputsMask is not ALL,
        // always try to check and set the availability of another input
        //为了避免饥饿，如果输入选择是 ALL 并且 availableInputsMask 不是 ALL，请始终尝试检查并设置另一个输入的可用性
        if (inputSelectionHandler.shouldSetAvailableForAnotherInput()) {
            fullCheckAndSetAvailable();
        }

        return readingInputIndex;
    }

    private void fullCheckAndSetAvailable() {
        for (int i = 0; i < inputProcessors.length; i++) {
            StreamOneInputProcessor<?> inputProcessor = inputProcessors[i];
            // TODO: isAvailable() can be a costly operation (checking volatile). If one of
            // the input is constantly available and another is not, we will be checking this
            // volatile
            // once per every record. This might be optimized to only check once per processed
            // NetworkBuffer
            //isAvailable() 可能是一项成本高昂的操作（检查 volatile）。
            // 如果其中一个输入始终可用，而另一个输入则不可用，我们将在每条记录中检查一次此易失性。
            // 这可能会被优化为每个处理的 NetworkBuffer 仅检查一次
            if (inputProcessor.isApproximatelyAvailable() || inputProcessor.isAvailable()) {
                inputSelectionHandler.setAvailableInput(i);
            }
        }
    }
}
