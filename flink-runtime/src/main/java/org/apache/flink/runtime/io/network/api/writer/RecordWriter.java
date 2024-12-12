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

package org.apache.flink.runtime.io.network.api.writer;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.AvailabilityProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.util.XORShiftRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * An abstract record-oriented runtime result writer.
 *
 * <p>The RecordWriter wraps the runtime's {@link ResultPartitionWriter} and takes care of
 * subpartition selection and serializing records into bytes.
 *
 * @param <T> the type of the record that can be emitted with this record writer
 */
//一个抽象的面向记录的运行时结果编写器。
//RecordWriter包装运行时的ResultPartitionWriter ，并负责子分区选择和将记录序列化为字节。
public abstract class RecordWriter<T extends IOReadableWritable> implements AvailabilityProvider {

    /** Default name for the output flush thread, if no name with a task reference is given. */
    //如果未给出带有任务引用的名称，则输出刷新线程的默认名称。
    @VisibleForTesting
    public static final String DEFAULT_OUTPUT_FLUSH_THREAD_NAME = "OutputFlusher";

    private static final Logger LOG = LoggerFactory.getLogger(RecordWriter.class);

    //结果分区，每一个算子实例Task都包含一个结果分区，结果分区的数量即是算子的并行度。
    protected final ResultPartitionWriter targetPartition;

    //结果子分区个数，对应下游有多少个算子实例Task消费该Task实例的数据。
    protected final int numberOfSubpartitions;

    //数据输出视图，包含一个byte数组，负责序列化StreamRecord数据元素并顺序写入byte数组中，最后封装出一个ByteBuffer内存区域。
    protected final DataOutputSerializer serializer;

    protected final Random rng = new XORShiftRandom();

    protected final boolean flushAlways;

    /** The thread that periodically flushes the output, to give an upper latency bound. */
    //定时刷新器，是一个单独的线程实现。
    //当上游数据产出较慢时，该线程负责以固定的时间间隔将已有的buffer数据发送到下游，避免下游算子等待过长时间。
    @Nullable private final OutputFlusher outputFlusher;

    /**
     * To avoid synchronization overhead on the critical path, best-effort error tracking is enough
     * here.
     */
    //为了避免关键路径上的同步开销，尽力而为的错误跟踪在这里就足够了。
    private Throwable flusherException;

    private volatile Throwable volatileFlusherException;
    private int volatileFlusherExceptionCheckSkipCount;
    private static final int VOLATILE_FLUSHER_EXCEPTION_MAX_CHECK_SKIP_COUNT = 100;

    RecordWriter(ResultPartitionWriter writer, long timeout, String taskName) {
        this.targetPartition = writer;
        this.numberOfSubpartitions = writer.getNumberOfSubpartitions();

        this.serializer = new DataOutputSerializer(128);

        checkArgument(timeout >= ExecutionOptions.DISABLED_NETWORK_BUFFER_TIMEOUT);
        this.flushAlways = (timeout == ExecutionOptions.FLUSH_AFTER_EVERY_RECORD);
        if (timeout == ExecutionOptions.DISABLED_NETWORK_BUFFER_TIMEOUT
                || timeout == ExecutionOptions.FLUSH_AFTER_EVERY_RECORD) {
            outputFlusher = null;
        } else {
            String threadName =
                    taskName == null
                            ? DEFAULT_OUTPUT_FLUSH_THREAD_NAME
                            : DEFAULT_OUTPUT_FLUSH_THREAD_NAME + " for " + taskName;

            outputFlusher = new OutputFlusher(threadName, timeout);
            outputFlusher.start();
        }
    }

    public void emit(T record, int targetSubpartition) throws IOException {
        checkErroneous();

        //将给定的序列化记录写入目标子分区。
        targetPartition.emitRecord(
                //先将StreamRecord数据元素序列化写入到ByteBuffer
                serializeRecord(serializer, record), targetSubpartition);

        if (flushAlways) {
            targetPartition.flush(targetSubpartition);
        }
    }

    protected void emit(ByteBuffer record, int targetSubpartition) throws IOException {
        checkErroneous();

        targetPartition.emitRecord(record, targetSubpartition);

        if (flushAlways) {
            targetPartition.flush(targetSubpartition);
        }
    }

    public void broadcastEvent(AbstractEvent event) throws IOException {
        broadcastEvent(event, false);
    }

    public void broadcastEvent(AbstractEvent event, boolean isPriorityEvent) throws IOException {
        targetPartition.broadcastEvent(event, isPriorityEvent);

        if (flushAlways) {
            flushAll();
        }
    }

    public void alignedBarrierTimeout(long checkpointId) throws IOException {
        targetPartition.alignedBarrierTimeout(checkpointId);
    }

    public void abortCheckpoint(long checkpointId, CheckpointException cause) {
        targetPartition.abortCheckpoint(checkpointId, cause);
    }

    @VisibleForTesting
    public static ByteBuffer serializeRecord(
            DataOutputSerializer serializer, IOReadableWritable record) throws IOException {
        // the initial capacity should be no less than 4 bytes
        //初始容量应不少于4字节
        serializer.setPositionUnsafe(4);

        // write data
        //写入数据
        record.write(serializer);

        // write length
        //写入长度
        serializer.writeIntUnsafe(serializer.length() - 4, 0);

        return serializer.wrapAsByteBuffer();
    }

    public void flushAll() {
        targetPartition.flushAll();
    }

    /** Sets the metric group for this RecordWriter. */
    //设置此 RecordWriter 的指标组。
    public void setMetricGroup(TaskIOMetricGroup metrics) {
        targetPartition.setMetricGroup(metrics);
    }

    public int getNumberOfSubpartitions() {
        return numberOfSubpartitions;
    }

    /**
     * Whether the subpartition where an element comes from can be derived from the existing
     * information. If false, the caller of this writer should attach the subpartition information
     * onto an element before writing it to a subpartition, if the element needs this information
     * afterward.
     */
    //是否可以从现有信息中推导出某个元素来自哪个子分区。
    //如果为 false，则此写入器的调用方应在将元素写入子分区之前将其附加到元素（如果该元素随后需要此信息）。
    public boolean isSubpartitionDerivable() {
        return !(targetPartition instanceof ResultPartition
                && ((ResultPartition) targetPartition).isNumberOfPartitionConsumerUndefined());
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        return targetPartition.getAvailableFuture();
    }

    /** This is used to send regular records. */
    //这用于发送常规记录。
    public abstract void emit(T record) throws IOException;

    /** This is used to send LatencyMarks to a random target subpartition. */
    //这用于将 LatencyMark 发送到随机目标子分区
    public void randomEmit(T record) throws IOException {
        checkErroneous();

        int targetSubpartition = rng.nextInt(numberOfSubpartitions);
        emit(record, targetSubpartition);
    }

    /** This is used to broadcast streaming Watermarks in-band with records. */
    //这用于在带内广播带有记录的流水印。
    public abstract void broadcastEmit(T record) throws IOException;

    /** Closes the writer. This stops the flushing thread (if there is one). */
    //关闭。这会停止冲洗线程（如果有的话）。
    public void close() {
        // make sure we terminate the thread in any case
        //确保我们在任何情况下都终止线程
        if (outputFlusher != null) {
            outputFlusher.terminate();
            try {
                outputFlusher.join();
            } catch (InterruptedException e) {
                // ignore on close
                // restore interrupt flag to fast exit further blocking calls
                //忽略关闭恢复中断标志以快速退出进一步的阻塞调用
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Notifies the writer that the output flusher thread encountered an exception.
     *
     * @param t The exception to report.
     */
    //通知编写器输出刷新器线程遇到异常
    private void notifyFlusherException(Throwable t) {
        if (flusherException == null) {
            LOG.error("An exception happened while flushing the outputs", t);
            flusherException = t;
            volatileFlusherException = t;
        }
    }

    protected void checkErroneous() throws IOException {
        // For performance reasons, we are not checking volatile field every single time.
        //出于性能原因，我们不会每次都检查易失性字段。
        if (flusherException != null
                || (volatileFlusherExceptionCheckSkipCount
                                >= VOLATILE_FLUSHER_EXCEPTION_MAX_CHECK_SKIP_COUNT
                        && volatileFlusherException != null)) {
            throw new IOException(
                    "An exception happened while flushing the outputs", volatileFlusherException);
        }
        if (++volatileFlusherExceptionCheckSkipCount
                >= VOLATILE_FLUSHER_EXCEPTION_MAX_CHECK_SKIP_COUNT) {
            volatileFlusherExceptionCheckSkipCount = 0;
        }
    }

    /** Sets the max overdraft buffer size of per gate. */
    //设置每个门的最大透支缓冲区大小。
    public void setMaxOverdraftBuffersPerGate(int maxOverdraftBuffersPerGate) {
        targetPartition.setMaxOverdraftBuffersPerGate(maxOverdraftBuffersPerGate);
    }

    // ------------------------------------------------------------------------

    /**
     * A dedicated thread that periodically flushes the output buffers, to set upper latency bounds.
     *
     * <p>The thread is daemonic, because it is only a utility thread.
     */
    //定期刷新输出缓冲区的专用线程，以设置延迟上限。
    //该线程是守护线程，因为它只是一个实用程序线程。
    private class OutputFlusher extends Thread {

        private final long timeout;

        private volatile boolean running = true;

        OutputFlusher(String name, long timeout) {
            super(name);
            setDaemon(true);
            this.timeout = timeout;
        }

        public void terminate() {
            running = false;
            interrupt();
        }

        @Override
        public void run() {
            try {
                while (running) {
                    try {
                        Thread.sleep(timeout);
                    } catch (InterruptedException e) {
                        // propagate this if we are still running, because it should not happen
                        // in that case
                        //如果我们仍在运行，则传播此信息，因为在这种情况下它不应该发生
                        if (running) {
                            throw new Exception(e);
                        }
                    }

                    // any errors here should let the thread come to a halt and be
                    // recognized by the writer
                    //这里的任何错误都应该让线程停止并被作者识别
                    flushAll();
                }
            } catch (Throwable t) {
                notifyFlusherException(t);
            }
        }
    }

    @VisibleForTesting
    ResultPartitionWriter getTargetPartition() {
        return targetPartition;
    }
}
