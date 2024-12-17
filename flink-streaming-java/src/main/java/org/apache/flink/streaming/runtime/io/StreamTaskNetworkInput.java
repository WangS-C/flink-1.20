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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.checkpointing.CheckpointedInputGate;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.tasks.StreamTask.CanEmitBatchOfRecordsChecker;
import org.apache.flink.streaming.runtime.watermarkstatus.StatusWatermarkValve;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

/**
 * Implementation of {@link StreamTaskInput} that wraps an input from network taken from {@link
 * CheckpointedInputGate}.
 *
 * <p>This internally uses a {@link StatusWatermarkValve} to keep track of {@link Watermark} and
 * {@link WatermarkStatus} events, and forwards them to event subscribers once the {@link
 * StatusWatermarkValve} determines the {@link Watermark} from all inputs has advanced, or that a
 * {@link WatermarkStatus} needs to be propagated downstream to denote a status change.
 *
 * <p>Forwarding elements, watermarks, or status elements must be protected by synchronizing on the
 * given lock object. This ensures that we don't call methods on a {@link StreamInputProcessor}
 * concurrently with the timer callback or other things.
 */
//StreamTaskInput的实现，包装来自CheckpointedInputGate的网络输入。
//这在内部使用StatusWatermarkValve来跟踪Watermark和WatermarkStatus事件，
// 并在StatusWatermarkValve确定所有输入中的Watermark已提前，
// 或者WatermarkStatus需要向下游传播以表示状态更改时将它们转发给事件订阅者。
//转发元素、水印或状态元素必须通过在给定锁定对象上同步来进行保护。
// 这确保我们不会与计时器回调或其他事情同时调用StreamInputProcessor上的方法。
@Internal
public final class StreamTaskNetworkInput<T>
        extends AbstractStreamTaskNetworkInput<
                T,
                SpillingAdaptiveSpanningRecordDeserializer<
                        DeserializationDelegate<StreamElement>>> {

    public StreamTaskNetworkInput(
            CheckpointedInputGate checkpointedInputGate,
            TypeSerializer<T> inputSerializer,
            IOManager ioManager,
            StatusWatermarkValve statusWatermarkValve,
            int inputIndex,
            CanEmitBatchOfRecordsChecker canEmitBatchOfRecords) {
        super(
                checkpointedInputGate,
                inputSerializer,
                statusWatermarkValve,
                inputIndex,
                getRecordDeserializers(checkpointedInputGate, ioManager),
                canEmitBatchOfRecords);
    }

    // Initialize one deserializer per input channel
    // 每个输入通道初始化一个解串器
    private static Map<
                    InputChannelInfo,
                    SpillingAdaptiveSpanningRecordDeserializer<
                            DeserializationDelegate<StreamElement>>>
            getRecordDeserializers(
                    CheckpointedInputGate checkpointedInputGate, IOManager ioManager) {
        return checkpointedInputGate.getChannelInfos().stream()
                .collect(
                        toMap(
                                identity(),
                                unused ->
                                        new SpillingAdaptiveSpanningRecordDeserializer<>(
                                                ioManager.getSpillingDirectoriesPaths())));
    }

    @Override
    public CompletableFuture<Void> prepareSnapshot(
            ChannelStateWriter channelStateWriter, long checkpointId) throws CheckpointException {
        for (Map.Entry<
                        InputChannelInfo,
                        SpillingAdaptiveSpanningRecordDeserializer<
                                DeserializationDelegate<StreamElement>>>
                e : recordDeserializers.entrySet()) {

            try {
                channelStateWriter.addInputData(
                        checkpointId,
                        e.getKey(),
                        ChannelStateWriter.SEQUENCE_NUMBER_UNKNOWN,
                        e.getValue().getUnconsumedBuffer());
            } catch (IOException ioException) {
                throw new CheckpointException(CheckpointFailureReason.IO_EXCEPTION, ioException);
            }
        }
        return checkpointedInputGate.getAllBarriersReceivedFuture(checkpointId);
    }

    @Override
    public void close() throws IOException {
        super.close();

        // cleanup the resources of the checkpointed input gate
        // 清理检查点输入门的资源
        checkpointedInputGate.close();
    }
}
