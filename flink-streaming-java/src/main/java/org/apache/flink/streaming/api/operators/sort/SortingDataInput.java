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

package org.apache.flink.streaming.api.operators.sort;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.AlgorithmOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.io.AvailabilityProvider;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.jobgraph.tasks.TaskInvokable;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.sort.ExternalSorter;
import org.apache.flink.runtime.operators.sort.PushSorter;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.DataInputStatus;
import org.apache.flink.streaming.runtime.io.StreamTaskInput;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.MutableObjectIterator;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * A {@link StreamTaskInput} which sorts in the incoming records from a chained input. It postpones
 * emitting the records until it receives {@link DataInputStatus#END_OF_INPUT} from the chained
 * input. After it is done it emits a single record at a time from the sorter.
 *
 * <p>The sorter uses binary comparison of keys, which are extracted and serialized when received
 * from the chained input. Moreover the timestamps of incoming records are used for secondary
 * ordering. For the comparison it uses either {@link FixedLengthByteKeyComparator} if the length of
 * the serialized key is constant, or {@link VariableLengthByteKeyComparator} otherwise.
 *
 * <p>Watermarks, watermark statuses, nor latency markers are propagated downstream as they do not
 * make sense with buffered records. The input emits the largest watermark seen after all records.
 *
 * @param <T> The type of the value in incoming {@link StreamRecord StreamRecords}.
 * @param <K> The type of the key.
 */
//streamTaskInput对来自链式输入的传入记录进行排序。
// 它会推迟发出记录，直到从链接输入接收到DataInputStatus. END_OF_INPUT 。完成后，它一次从排序器中发出一条记录。
//排序器使用键的二进制比较，从链式输入接收到键时将其提取并序列化。
// 此外，传入记录的时间戳用于二次排序。
// 对于比较，如果序列化密钥的长度是常量，
//则使用固定长度字节FixedLengthByteKeyComparator ，否则使用可变VariableLengthByteKeyComparator 。
//水印、水印状态或延迟标记都会向下游传播，因为它们对缓冲记录没有意义。输入发出所有记录之后看到的最大水印。
//类型参数：
//< T > – 传入StreamRecords中的值的类型。 < K > – 密钥的类型。
public final class SortingDataInput<T, K> implements StreamTaskInput<T> {

    private final StreamTaskInput<T> wrappedInput;
    private final PushSorter<Tuple2<byte[], StreamRecord<T>>> sorter;
    private final KeySelector<T, K> keySelector;
    private final TypeSerializer<K> keySerializer;
    private final DataOutputSerializer dataOutputSerializer;
    private final ForwardingDataOutput forwardingDataOutput;
    private MutableObjectIterator<Tuple2<byte[], StreamRecord<T>>> sortedInput = null;
    private boolean emittedLast;
    private long watermarkSeen = Long.MIN_VALUE;

    public SortingDataInput(
            StreamTaskInput<T> wrappedInput,
            TypeSerializer<T> typeSerializer,
            TypeSerializer<K> keySerializer,
            KeySelector<T, K> keySelector,
            MemoryManager memoryManager,
            IOManager ioManager,
            boolean objectReuse,
            double managedMemoryFraction,
            Configuration taskManagerConfiguration,
            TaskInvokable containingTask,
            ExecutionConfig executionConfig) {
        try {
            this.forwardingDataOutput = new ForwardingDataOutput();
            this.keySelector = keySelector;
            this.keySerializer = keySerializer;
            int keyLength = keySerializer.getLength();
            final TypeComparator<Tuple2<byte[], StreamRecord<T>>> comparator;
            if (keyLength > 0) {
                this.dataOutputSerializer = new DataOutputSerializer(keyLength);
                comparator = new FixedLengthByteKeyComparator<>(keyLength);
            } else {
                this.dataOutputSerializer = new DataOutputSerializer(64);
                comparator = new VariableLengthByteKeyComparator<>();
            }
            KeyAndValueSerializer<T> keyAndValueSerializer =
                    new KeyAndValueSerializer<>(typeSerializer, keyLength);
            this.wrappedInput = wrappedInput;
            this.sorter =
                    ExternalSorter.newBuilder(
                                    memoryManager,
                                    containingTask,
                                    keyAndValueSerializer,
                                    comparator,
                                    executionConfig)
                            .memoryFraction(managedMemoryFraction)
                            .enableSpilling(
                                    ioManager,
                                    taskManagerConfiguration.get(
                                            AlgorithmOptions.SORT_SPILLING_THRESHOLD))
                            .maxNumFileHandles(
                                    taskManagerConfiguration.get(AlgorithmOptions.SPILLING_MAX_FAN))
                            .objectReuse(objectReuse)
                            .largeRecords(
                                    taskManagerConfiguration.get(
                                            AlgorithmOptions.USE_LARGE_RECORDS_HANDLER))
                            .build();
        } catch (MemoryAllocationException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int getInputIndex() {
        return wrappedInput.getInputIndex();
    }

    @Override
    public CompletableFuture<Void> prepareSnapshot(
            ChannelStateWriter channelStateWriter, long checkpointId) throws CheckpointException {
        throw new UnsupportedOperationException(
                "Checkpoints are not supported with sorted inputs" + " in the BATCH runtime.");
    }

    @Override
    public void close() throws IOException {
        IOException ex = null;
        try {
            wrappedInput.close();
        } catch (IOException e) {
            ex = ExceptionUtils.firstOrSuppressed(e, ex);
        }

        try {
            sorter.close();
        } catch (IOException e) {
            ex = ExceptionUtils.firstOrSuppressed(e, ex);
        }

        if (ex != null) {
            throw ex;
        }
    }

    private class ForwardingDataOutput implements DataOutput<T> {
        @Override
        public void emitRecord(StreamRecord<T> streamRecord) throws Exception {
            K key = keySelector.getKey(streamRecord.getValue());

            keySerializer.serialize(key, dataOutputSerializer);
            byte[] serializedKey = dataOutputSerializer.getCopyOfBuffer();
            dataOutputSerializer.clear();

            sorter.writeRecord(Tuple2.of(serializedKey, streamRecord));
        }

        @Override
        public void emitWatermark(Watermark watermark) {
            watermarkSeen = Math.max(watermarkSeen, watermark.getTimestamp());
        }

        @Override
        public void emitWatermarkStatus(WatermarkStatus watermarkStatus) {}

        @Override
        public void emitLatencyMarker(LatencyMarker latencyMarker) {}

        @Override
        public void emitRecordAttributes(RecordAttributes recordAttributes) {
            // The SortingDataInput is only used in batch execution mode. The RecordAttributes is
            // not used in batch execution mode. We will ignore all the RecordAttributes.
            //SortingDataInput 仅在批处理执行模式下使用。
            //RecordAttributes 不用于批处理执行模式。我们将忽略所有 RecordAttributes。
        }
    }

    @Override
    public DataInputStatus emitNext(DataOutput<T> output) throws Exception {
        if (sortedInput != null) {
            return emitNextSortedRecord(output);
        }

        DataInputStatus inputStatus = wrappedInput.emitNext(forwardingDataOutput);
        if (inputStatus == DataInputStatus.END_OF_DATA) {
            endSorting();
            return emitNextSortedRecord(output);
        }

        return inputStatus;
    }

    @Nonnull
    private DataInputStatus emitNextSortedRecord(DataOutput<T> output) throws Exception {
        if (emittedLast) {
            return DataInputStatus.END_OF_INPUT;
        }

        Tuple2<byte[], StreamRecord<T>> next = sortedInput.next();
        if (next != null) {
            output.emitRecord(next.f1);
            return DataInputStatus.MORE_AVAILABLE;
        } else {
            emittedLast = true;
            if (watermarkSeen > Long.MIN_VALUE) {
                output.emitWatermark(new Watermark(watermarkSeen));
            }
            return DataInputStatus.END_OF_DATA;
        }
    }

    private void endSorting() throws Exception {
        this.sorter.finishReading();
        this.sortedInput = sorter.getIterator();
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        if (sortedInput != null) {
            return AvailabilityProvider.AVAILABLE;
        } else {
            return wrappedInput.getAvailableFuture();
        }
    }
}
