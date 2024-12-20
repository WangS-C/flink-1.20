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

import org.apache.flink.api.connector.source.ExternallyInducedSourceReader;
import org.apache.flink.streaming.api.operators.SourceOperator;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/** A subclass of {@link StreamTaskSourceInput} for {@link ExternallyInducedSourceReader}. */
//ExternallyInducedSourceReader的StreamTaskSourceInput的子类。
public class StreamTaskExternallyInducedSourceInput<T> extends StreamTaskSourceInput<T> {
    private final Consumer<Long> checkpointTriggeringHook;
    private final ExternallyInducedSourceReader<T, ?> sourceReader;
    private CompletableFuture<?> blockFuture;

    @SuppressWarnings("unchecked")
    public StreamTaskExternallyInducedSourceInput(
            SourceOperator<T, ?> operator,
            Consumer<Long> checkpointTriggeringHook,
            int inputGateIndex,
            int inputIndex) {
        super(operator, inputGateIndex, inputIndex);
        this.checkpointTriggeringHook = checkpointTriggeringHook;
        this.sourceReader = (ExternallyInducedSourceReader<T, ?>) operator.getSourceReader();
    }

    public void blockUntil(CompletableFuture<?> blockFuture) {
        this.blockFuture = blockFuture;
        // assume that the future is completed in mailbox thread
        // 假设 future 在mailbox线程中完成
        blockFuture.whenComplete((v, e) -> unblock());
    }

    private void unblock() {
        this.blockFuture = null;
    }

    @Override
    public DataInputStatus emitNext(DataOutput<T> output) throws Exception {
        if (blockFuture != null) {
            return DataInputStatus.NOTHING_AVAILABLE;
        }

        DataInputStatus status = super.emitNext(output);
        if (status == DataInputStatus.NOTHING_AVAILABLE) {
            sourceReader.shouldTriggerCheckpoint().ifPresent(checkpointTriggeringHook);
        }
        return status;
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        if (blockFuture != null) {
            return blockFuture;
        }
        return super.getAvailableFuture();
    }
}
