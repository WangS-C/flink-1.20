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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.metrics.TimerGauge;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkElementIndex;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link ResultPartition} which writes buffers directly to {@link ResultSubpartition}s. This is
 * in contrast to implementations where records are written to a joint structure, from which the
 * subpartitions draw the data after the write phase is finished, for example the sort-based
 * partitioning.
 *
 * <p>To avoid confusion: On the read side, all subpartitions return buffers (and backlog) to be
 * transported through the network.
 */
//将缓冲区直接写入ResultSubpartition s的ResultPartition。
//这与将记录写入到联合结构的实现形成对比，在写入阶段完成之后，子分区从联合结构中绘制数据，例如基于排序的分区。
//为了避免混淆: 在读取端，所有子分区都返回要通过网络传输的缓冲区 (和积压)。
public abstract class BufferWritingResultPartition extends ResultPartition {

    /** The subpartitions of this partition. At least one. */
    //此分区的子分区。至少一个
    //结果子分区数组
    protected final ResultSubpartition[] subpartitions;

    /**
     * For non-broadcast mode, each subpartition maintains a separate BufferBuilder which might be
     * null.
     */
    //对于非广播模式，每个子分区都维护一个单独的BufferBuilder，该BufferBuilder可能为null。
    private final BufferBuilder[] unicastBufferBuilders;

    /** For broadcast mode, a single BufferBuilder is shared by all subpartitions. */
    //对于广播模式，单个BufferBuilder由所有子分区共享。
    private BufferBuilder broadcastBufferBuilder;

    private TimerGauge hardBackPressuredTimeMsPerSecond = new TimerGauge();

    private long totalWrittenBytes;

    public BufferWritingResultPartition(
            String owningTaskName,
            int partitionIndex,
            ResultPartitionID partitionId,
            ResultPartitionType partitionType,
            ResultSubpartition[] subpartitions,
            int numTargetKeyGroups,
            ResultPartitionManager partitionManager,
            @Nullable BufferCompressor bufferCompressor,
            SupplierWithException<BufferPool, IOException> bufferPoolFactory) {

        super(
                owningTaskName,
                partitionIndex,
                partitionId,
                partitionType,
                subpartitions.length,
                numTargetKeyGroups,
                partitionManager,
                bufferCompressor,
                bufferPoolFactory);

        this.subpartitions = checkNotNull(subpartitions);
        this.unicastBufferBuilders = new BufferBuilder[subpartitions.length];
    }

    @Override
    protected void setupInternal() throws IOException {
        checkState(
                bufferPool.getNumberOfRequiredMemorySegments() >= getNumberOfSubpartitions(),
                "Bug in result partition setup logic: Buffer pool has not enough guaranteed buffers for"
                        + " this result partition.");
    }

    @Override
    public int getNumberOfQueuedBuffers() {
        int totalBuffers = 0;

        for (ResultSubpartition subpartition : subpartitions) {
            totalBuffers += subpartition.unsynchronizedGetNumberOfQueuedBuffers();
        }

        return totalBuffers;
    }

    @Override
    public long getSizeOfQueuedBuffersUnsafe() {
        long totalNumberOfBytes = 0;

        for (ResultSubpartition subpartition : subpartitions) {
            totalNumberOfBytes += Math.max(0, subpartition.getTotalNumberOfBytesUnsafe());
        }

        return totalWrittenBytes - totalNumberOfBytes;
    }

    @Override
    public int getNumberOfQueuedBuffers(int targetSubpartition) {
        checkArgument(targetSubpartition >= 0 && targetSubpartition < numSubpartitions);
        return subpartitions[targetSubpartition].unsynchronizedGetNumberOfQueuedBuffers();
    }

    protected void flushSubpartition(int targetSubpartition, boolean finishProducers) {
        if (finishProducers) {
            finishBroadcastBufferBuilder();
            finishUnicastBufferBuilder(targetSubpartition);
        }

        subpartitions[targetSubpartition].flush();
    }

    protected void flushAllSubpartitions(boolean finishProducers) {
        if (finishProducers) {
            finishBroadcastBufferBuilder();
            finishUnicastBufferBuilders();
        }

        for (ResultSubpartition subpartition : subpartitions) {
            subpartition.flush();
        }
    }

    //方法实现中会先从LocalBufferPool资源池中获取Buffer资源并将ByteBuffer数据写入。
    //BufferBuilder底层用MemorySegment代表Buffer资源信息。
    //如果ByteBuffer数据过大，一个BufferBuilder被写满后还有剩余数据，
    //则将该BufferBuilder记为finish状态，继续申请新BufferBuilder实例，直到把剩余数据写完。
    @Override
    public void emitRecord(ByteBuffer record, int targetSubpartition) throws IOException {
        totalWrittenBytes += record.remaining();

        //分配新的BufferBuilder实例并加入结果子分区buffers队列，最后将ByteBuffer数据写入到BufferBuilder实例中。
        BufferBuilder buffer = appendUnicastDataForNewRecord(record, targetSubpartition);

        while (record.hasRemaining()) {
            // full buffer, partial record
            //缓冲区已满，部分记录
            finishUnicastBufferBuilder(targetSubpartition);
            buffer = appendUnicastDataForRecordContinuation(record, targetSubpartition);
        }

        if (buffer.isFull()) {
            // full buffer, full record
            finishUnicastBufferBuilder(targetSubpartition);
        }

        // partial buffer, full record
    }

    @Override
    public void broadcastRecord(ByteBuffer record) throws IOException {
        totalWrittenBytes += ((long) record.remaining() * numSubpartitions);

        BufferBuilder buffer = appendBroadcastDataForNewRecord(record);

        while (record.hasRemaining()) {
            // full buffer, partial record
            finishBroadcastBufferBuilder();
            buffer = appendBroadcastDataForRecordContinuation(record);
        }

        if (buffer.isFull()) {
            // full buffer, full record
            finishBroadcastBufferBuilder();
        }

        // partial buffer, full record
    }

    @Override
    public void broadcastEvent(AbstractEvent event, boolean isPriorityEvent) throws IOException {
        checkInProduceState();
        finishBroadcastBufferBuilder();
        finishUnicastBufferBuilders();

        try (BufferConsumer eventBufferConsumer =
                EventSerializer.toBufferConsumer(event, isPriorityEvent)) {
            totalWrittenBytes += ((long) eventBufferConsumer.getWrittenBytes() * numSubpartitions);
            for (ResultSubpartition subpartition : subpartitions) {
                // Retain the buffer so that it can be recycled by each subpartition of
                // targetPartition
                subpartition.add(eventBufferConsumer.copy(), 0);
            }
        }
    }

    @Override
    public void alignedBarrierTimeout(long checkpointId) throws IOException {
        for (ResultSubpartition subpartition : subpartitions) {
            subpartition.alignedBarrierTimeout(checkpointId);
        }
    }

    @Override
    public void abortCheckpoint(long checkpointId, CheckpointException cause) {
        for (ResultSubpartition subpartition : subpartitions) {
            subpartition.abortCheckpoint(checkpointId, cause);
        }
    }

    @Override
    public void setMetricGroup(TaskIOMetricGroup metrics) {
        super.setMetricGroup(metrics);
        hardBackPressuredTimeMsPerSecond = metrics.getHardBackPressuredTimePerSecond();
    }

    @Override
    protected ResultSubpartitionView createSubpartitionView(
            int subpartitionIndex, BufferAvailabilityListener availabilityListener)
            throws IOException {
        checkElementIndex(subpartitionIndex, numSubpartitions, "Subpartition not found.");
        checkState(!isReleased(), "Partition released.");

        ResultSubpartition subpartition = subpartitions[subpartitionIndex];
        //创建读取视图
        ResultSubpartitionView readView = subpartition.createReadView(availabilityListener);

        LOG.debug("Created {}", readView);

        return readView;
    }

    @Override
    public void finish() throws IOException {
        finishBroadcastBufferBuilder();
        finishUnicastBufferBuilders();

        for (ResultSubpartition subpartition : subpartitions) {
            totalWrittenBytes += subpartition.finish();
        }

        super.finish();
    }

    @Override
    protected void releaseInternal() {
        // Release all subpartitions
        for (ResultSubpartition subpartition : subpartitions) {
            try {
                subpartition.release();
            }
            // Catch this in order to ensure that release is called on all subpartitions
            catch (Throwable t) {
                LOG.error("Error during release of result subpartition: " + t.getMessage(), t);
            }
        }
    }

    @Override
    public void close() {
        // We can not close these buffers in the release method because of the potential race
        // condition. This close method will be only called from the Task thread itself.
        if (broadcastBufferBuilder != null) {
            broadcastBufferBuilder.close();
            broadcastBufferBuilder = null;
        }
        for (int i = 0; i < unicastBufferBuilders.length; ++i) {
            if (unicastBufferBuilders[i] != null) {
                unicastBufferBuilders[i].close();
                unicastBufferBuilders[i] = null;
            }
        }
        super.close();
    }

    private BufferBuilder appendUnicastDataForNewRecord(
            final ByteBuffer record, final int targetSubpartition) throws IOException {
        if (targetSubpartition < 0 || targetSubpartition > unicastBufferBuilders.length) {
            throw new ArrayIndexOutOfBoundsException(targetSubpartition);
        }
        BufferBuilder buffer = unicastBufferBuilders[targetSubpartition];

        if (buffer == null) {
            //请求新的
            buffer = requestNewUnicastBufferBuilder(targetSubpartition);
            //将数据Buffer存放到PipelinedSubpartition的buffers队列中。
            addToSubpartition(buffer, targetSubpartition, 0, record.remaining());
        }

        append(record, buffer);

        return buffer;
    }

    private int append(ByteBuffer record, BufferBuilder buffer) {
        // Try to avoid hard back-pressure in the subsequent calls to request buffers
        // by ignoring Buffer Debloater hints and extending the buffer if possible (trim).
        // This decreases the probability of hard back-pressure in cases when
        // the output size varies significantly and BD suggests too small values.
        // The hint will be re-applied on the next iteration.
        if (record.remaining() >= buffer.getWritableBytes()) {
            // This 2nd check is expensive, so it shouldn't be re-ordered.
            // However, it has the same cost as the subsequent call to request buffer, so it doesn't
            // affect the performance much.
            if (!bufferPool.isAvailable()) {
                // add 1 byte to prevent immediately flushing the buffer and potentially fit the
                // next record
                int newSize =
                        buffer.getMaxCapacity()
                                + (record.remaining() - buffer.getWritableBytes())
                                + 1;
                buffer.trim(Math.max(buffer.getMaxCapacity(), newSize));
            }
        }
        return buffer.appendAndCommit(record);
    }

    private void addToSubpartition(
            BufferBuilder buffer,
            int targetSubpartition,
            int partialRecordLength,
            int minDesirableBufferSize)
            throws IOException {
        int desirableBufferSize =
                //添加给定的缓冲区
                subpartitions[targetSubpartition].add(
                        buffer.createBufferConsumerFromBeginning(), partialRecordLength);

        resizeBuffer(buffer, desirableBufferSize, minDesirableBufferSize);
    }

    protected int addToSubpartition(
            int targetSubpartition, BufferConsumer bufferConsumer, int partialRecordLength)
            throws IOException {
        totalWrittenBytes += bufferConsumer.getWrittenBytes();
        return subpartitions[targetSubpartition].add(bufferConsumer, partialRecordLength);
    }

    private void resizeBuffer(
            BufferBuilder buffer, int desirableBufferSize, int minDesirableBufferSize) {
        if (desirableBufferSize > 0) {
            // !! If some of partial data has written already to this buffer, the result size can
            // not be less than written value.
            buffer.trim(Math.max(minDesirableBufferSize, desirableBufferSize));
        }
    }

    private BufferBuilder appendUnicastDataForRecordContinuation(
            final ByteBuffer remainingRecordBytes, final int targetSubpartition)
            throws IOException {
        final BufferBuilder buffer = requestNewUnicastBufferBuilder(targetSubpartition);
        // !! Be aware, in case of partialRecordBytes != 0, partial length and data has to
        // `appendAndCommit` first
        // before consumer is created. Otherwise it would be confused with the case the buffer
        // starting
        // with a complete record.
        // !! The next two lines can not change order.
        final int partialRecordBytes = append(remainingRecordBytes, buffer);
        addToSubpartition(buffer, targetSubpartition, partialRecordBytes, partialRecordBytes);

        return buffer;
    }

    private BufferBuilder appendBroadcastDataForNewRecord(final ByteBuffer record)
            throws IOException {
        BufferBuilder buffer = broadcastBufferBuilder;

        if (buffer == null) {
            buffer = requestNewBroadcastBufferBuilder();
            createBroadcastBufferConsumers(buffer, 0, record.remaining());
        }

        append(record, buffer);

        return buffer;
    }

    private BufferBuilder appendBroadcastDataForRecordContinuation(
            final ByteBuffer remainingRecordBytes) throws IOException {
        final BufferBuilder buffer = requestNewBroadcastBufferBuilder();
        // !! Be aware, in case of partialRecordBytes != 0, partial length and data has to
        // `appendAndCommit` first
        // before consumer is created. Otherwise it would be confused with the case the buffer
        // starting
        // with a complete record.
        // !! The next two lines can not change order.
        final int partialRecordBytes = append(remainingRecordBytes, buffer);
        createBroadcastBufferConsumers(buffer, partialRecordBytes, partialRecordBytes);

        return buffer;
    }

    private void createBroadcastBufferConsumers(
            BufferBuilder buffer, int partialRecordBytes, int minDesirableBufferSize)
            throws IOException {
        try (final BufferConsumer consumer = buffer.createBufferConsumerFromBeginning()) {
            int desirableBufferSize = Integer.MAX_VALUE;
            for (ResultSubpartition subpartition : subpartitions) {
                int subPartitionBufferSize = subpartition.add(consumer.copy(), partialRecordBytes);
                if (subPartitionBufferSize != ResultSubpartition.ADD_BUFFER_ERROR_CODE) {
                    desirableBufferSize = Math.min(desirableBufferSize, subPartitionBufferSize);
                }
            }
            resizeBuffer(buffer, desirableBufferSize, minDesirableBufferSize);
        }
    }

    private BufferBuilder requestNewUnicastBufferBuilder(int targetSubpartition)
            throws IOException {
        checkInProduceState();
        ensureUnicastMode();
        //从LocalBufferPool中申请新的BufferBuilder资源。
        final BufferBuilder bufferBuilder = requestNewBufferBuilderFromPool(targetSubpartition);
        unicastBufferBuilders[targetSubpartition] = bufferBuilder;

        return bufferBuilder;
    }

    private BufferBuilder requestNewBroadcastBufferBuilder() throws IOException {
        checkInProduceState();
        ensureBroadcastMode();

        final BufferBuilder bufferBuilder = requestNewBufferBuilderFromPool(0);
        broadcastBufferBuilder = bufferBuilder;
        return bufferBuilder;
    }

    private BufferBuilder requestNewBufferBuilderFromPool(int targetSubpartition)
            throws IOException {
        //请求一个BufferBuilder实例
        BufferBuilder bufferBuilder = bufferPool.requestBufferBuilder(targetSubpartition);
        if (bufferBuilder != null) {
            return bufferBuilder;
        }

        hardBackPressuredTimeMsPerSecond.markStart();
        try {
            bufferBuilder = bufferPool.requestBufferBuilderBlocking(targetSubpartition);
            hardBackPressuredTimeMsPerSecond.markEnd();
            return bufferBuilder;
        } catch (InterruptedException e) {
            throw new IOException("Interrupted while waiting for buffer");
        }
    }

    private void finishUnicastBufferBuilder(int targetSubpartition) {
        final BufferBuilder bufferBuilder = unicastBufferBuilders[targetSubpartition];
        if (bufferBuilder != null) {
            int bytes = bufferBuilder.finish();
            resultPartitionBytes.inc(targetSubpartition, bytes);
            numBytesOut.inc(bytes);
            numBuffersOut.inc();
            unicastBufferBuilders[targetSubpartition] = null;
            bufferBuilder.close();
        }
    }

    private void finishUnicastBufferBuilders() {
        for (int subpartition = 0; subpartition < numSubpartitions; subpartition++) {
            finishUnicastBufferBuilder(subpartition);
        }
    }

    private void finishBroadcastBufferBuilder() {
        if (broadcastBufferBuilder != null) {
            int bytes = broadcastBufferBuilder.finish();
            resultPartitionBytes.incAll(bytes);
            numBytesOut.inc(bytes * numSubpartitions);
            numBuffersOut.inc(numSubpartitions);
            broadcastBufferBuilder.close();
            broadcastBufferBuilder = null;
        }
    }

    private void ensureUnicastMode() {
        finishBroadcastBufferBuilder();
    }

    private void ensureBroadcastMode() {
        finishUnicastBufferBuilders();
    }

    @VisibleForTesting
    public TimerGauge getHardBackPressuredTimeMsPerSecond() {
        return hardBackPressuredTimeMsPerSecond;
    }

    @VisibleForTesting
    public ResultSubpartition[] getAllPartitions() {
        return subpartitions;
    }
}
