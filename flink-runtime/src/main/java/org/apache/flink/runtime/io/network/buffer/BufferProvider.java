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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.AvailabilityProvider;

import javax.annotation.Nullable;

/**
 * A buffer provider to request buffers from in a synchronous or asynchronous fashion.
 *
 * <p>The data producing side (result partition writers) request buffers in a synchronous fashion,
 * whereas the input side requests asynchronously.
 */
public interface BufferProvider extends AvailabilityProvider {

    /**
     * Returns a {@link Buffer} instance from the buffer provider, if one is available.
     *
     * @return {@code null} if no buffer is available or the buffer provider has been destroyed.
     */
    @Nullable
    Buffer requestBuffer();

    /**
     * Returns a {@link BufferBuilder} instance from the buffer provider. This equals to {@link
     * #requestBufferBuilder(int)} with unknown target channel.
     *
     * @return {@code null} if no buffer is available or the buffer provider has been destroyed.
     */
    @Nullable
    BufferBuilder requestBufferBuilder();

    /**
     * Returns a {@link BufferBuilder} instance from the buffer provider.
     *
     * @param targetChannel to which the request will be accounted to.
     * @return {@code null} if no buffer is available or the buffer provider has been destroyed.
     */
    //返回一个BufferBuilder实例从缓冲区提供程序。
    @Nullable
    BufferBuilder requestBufferBuilder(int targetChannel);

    /**
     * Returns a {@link BufferBuilder} instance from the buffer provider. This equals to {@link
     * #requestBufferBuilderBlocking(int)} with unknown target channel.
     *
     * <p>If there is no buffer available, the call will block until one becomes available again or
     * the buffer provider has been destroyed.
     */
    BufferBuilder requestBufferBuilderBlocking() throws InterruptedException;

    /**
     * Returns a {@link BufferBuilder} instance from the buffer provider.
     *
     * <p>If there is no buffer available, the call will block until one becomes available again or
     * the buffer provider has been destroyed.
     *
     * @param targetChannel to which the request will be accounted to.
     */
    BufferBuilder requestBufferBuilderBlocking(int targetChannel) throws InterruptedException;

    /**
     * Adds a buffer availability listener to the buffer provider.
     *
     * <p>The operation fails with return value <code>false</code>, when there is a buffer available
     * or the buffer provider has been destroyed.
     */
    boolean addBufferListener(BufferListener listener);

    /** Returns whether the buffer provider has been destroyed. */
    boolean isDestroyed();

    /**
     * Returns a {@link MemorySegment} instance from the buffer provider.
     *
     * @return {@code null} if no memory segment is available or the buffer provider has been
     *     destroyed.
     */
    @Nullable
    MemorySegment requestMemorySegment();

    /**
     * Returns a {@link MemorySegment} instance from the buffer provider.
     *
     * <p>If there is no memory segment available, the call will block until one becomes available
     * again or the buffer provider has been destroyed.
     */
    MemorySegment requestMemorySegmentBlocking() throws InterruptedException;
}
