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

/** Interface for recycling {@link MemorySegment}s. */
//用于回收MemorySegment的接口。
public interface BufferRecycler {

    /**
     * Recycles the {@link MemorySegment} to its original {@link BufferPool} instance.
     *
     * @param memorySegment The memory segment to be recycled.
     */
    void recycle(MemorySegment memorySegment);

    /** The buffer recycler does nothing for recycled segment. */
    final class DummyBufferRecycler implements BufferRecycler {

        public static final BufferRecycler INSTANCE = new DummyBufferRecycler();

        private DummyBufferRecycler() {}

        @Override
        public void recycle(MemorySegment memorySegment) {}
    }
}
