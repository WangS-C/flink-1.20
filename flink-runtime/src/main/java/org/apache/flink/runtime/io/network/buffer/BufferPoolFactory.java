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

package org.apache.flink.runtime.io.network.buffer;

import java.io.IOException;

/** A factory for buffer pools. */
public interface BufferPoolFactory {

    /**
     * Tries to create a buffer pool, which is guaranteed to provide at least the number of required
     * buffers.
     *
     * <p>The buffer pool is of dynamic size with at least <tt>numRequiredBuffers</tt> buffers.
     *
     * @param numRequiredBuffers minimum number of network buffers in this pool
     * @param maxUsedBuffers maximum number of network buffers this pool offers
     */
    BufferPool createBufferPool(int numRequiredBuffers, int maxUsedBuffers) throws IOException;

    /**
     * Tries to create a buffer pool with an owner, which is guaranteed to provide at least the
     * number of required buffers.
     *
     * <p>The buffer pool is of dynamic size with at least <tt>numRequiredBuffers</tt> buffers.
     *
     * @param numRequiredBuffers minimum number of network buffers in this pool
     * @param maxUsedBuffers maximum number of network buffers this pool offers
     * @param numSubpartitions number of subpartitions in this pool
     * @param maxBuffersPerChannel maximum number of buffers to use for each channel
     * @param maxOverdraftBuffersPerGate maximum number of overdraft buffers to use for each gate
     */
    //尝试创建具有所有者的缓冲池，该缓冲池保证至少提供所需数量的缓冲区。
    //缓冲池具有动态大小，至少包含numRequiredBuffers缓冲区。
    //参数:
    //numRequiredBuffers -此池 中网络缓冲区的最小数量
    //maxUsedBuffers -此池提供的网络缓冲区的最大数量
    //numSubpartitions -此池 子分区的数量
    //maxBuffersPerChannel -每个通道使用的缓冲区的最大数量
    //maxoverdraftbuffergate -透支的最大数量用于每个门的缓冲区
    BufferPool createBufferPool(
            int numRequiredBuffers,
            int maxUsedBuffers,
            int numSubpartitions,
            int maxBuffersPerChannel,
            int maxOverdraftBuffersPerGate)
            throws IOException;

    /** Destroy callback for updating factory book keeping. */
    void destroyBufferPool(BufferPool bufferPool) throws IOException;
}
