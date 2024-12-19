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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions.CompressionCodec;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.compression.BlockCompressionFactory;
import org.apache.flink.runtime.io.compression.BlockDecompressor;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** Decompressor for compressed {@link Buffer}. */
//压缩Buffer的解压缩器。
public class BufferDecompressor {

    /** The backing block decompressor for data decompression. */
    //用于数据解压缩的支持块解压缩器。
    private final BlockDecompressor blockDecompressor;

    /** The intermediate buffer for the decompressed data. */
    //解压缩数据的中间缓冲区。
    private final NetworkBuffer internalBuffer;

    /** The backup array of intermediate buffer. */
    //中间缓冲区的备份数组。
    private final byte[] internalBufferArray;

    public BufferDecompressor(int bufferSize, CompressionCodec factoryName) {
        checkArgument(bufferSize > 0);
        checkNotNull(factoryName);

        // the decompressed data size should be never larger than the configured buffer size
        //解压后的数据大小不应大于配置的缓冲区大小
        this.internalBufferArray = new byte[bufferSize];
        this.internalBuffer =
                new NetworkBuffer(
                        MemorySegmentFactory.wrap(internalBufferArray),
                        FreeingBufferRecycler.INSTANCE);
        this.blockDecompressor =
                BlockCompressionFactory.createBlockCompressionFactory(factoryName)
                        .getDecompressor();
    }

    /**
     * Decompresses the given {@link Buffer} using {@link BlockDecompressor}. The decompressed data
     * will be stored in the intermediate buffer of this {@link BufferDecompressor} and returned to
     * the caller. The caller must guarantee that the returned {@link Buffer} has been freed when
     * calling the method next time.
     *
     * <p>Notes that the decompression will always start from offset 0 to the size of the input
     * {@link Buffer}.
     */
    //使用BlockDecompressor解压缩给定的Buffer 。
    // 解压后的数据将存储在该BufferDecompressor的中间缓冲区中并返回给调用者。
    // 调用者必须保证下次调用该方法时返回的Buffer已被释放。
    //请注意，解压缩始终从偏移量 0 开始，直到输入Buffer的大小。
    public Buffer decompressToIntermediateBuffer(Buffer buffer) {
        int decompressedLen = decompress(buffer);
        internalBuffer.setSize(decompressedLen);

        return internalBuffer.retainBuffer();
    }

    /**
     * The difference between this method and {@link #decompressToIntermediateBuffer(Buffer)} is
     * that this method copies the decompressed data to the input {@link Buffer} starting from
     * offset 0.
     *
     * <p>The caller must guarantee that the input {@link Buffer} is writable and there's enough
     * space left.
     */
    //该方法与decompressToIntermediateBuffer(Buffer)的区别在于，该方法将解压后的数据从偏移量 0 开始复制到输入Buffer中。
    //调用者必须保证输入Buffer可写并且有足够的剩余空间。
    @VisibleForTesting
    public Buffer decompressToOriginalBuffer(Buffer buffer) {
        int decompressedLen = decompress(buffer);

        // copy the decompressed data back
        //将解压后的数据复制回来
        int memorySegmentOffset = buffer.getMemorySegmentOffset();
        MemorySegment segment = buffer.getMemorySegment();
        segment.put(memorySegmentOffset, internalBufferArray, 0, decompressedLen);

        return new ReadOnlySlicedNetworkBuffer(
                buffer.asByteBuf(), 0, decompressedLen, memorySegmentOffset, false);
    }

    /**
     * Decompresses the input {@link Buffer} into the intermediate buffer and returns the
     * decompressed data size.
     */
    //将输入Buffer解压到中间buffer中，并返回解压后的数据大小
    private int decompress(Buffer buffer) {
        checkArgument(buffer != null, "The input buffer must not be null.");
        checkArgument(buffer.isBuffer(), "Event can not be decompressed.");
        checkArgument(buffer.isCompressed(), "Buffer not compressed.");
        checkArgument(buffer.getReaderIndex() == 0, "Reader index of the input buffer must be 0.");
        checkArgument(buffer.readableBytes() > 0, "No data to be decompressed.");
        checkState(
                internalBuffer.refCnt() == 1,
                "Illegal reference count, buffer need to be released.");

        int length = buffer.getSize();
        MemorySegment memorySegment = buffer.getMemorySegment();
        // If buffer is on-heap, manipulate the underlying array directly. There are two main
        // reasons why NIO buffer is not directly used here: One is that some compression
        // libraries will use the underlying array for heap buffer, but our input buffer may be
        // a read-only ByteBuffer, and it is illegal to access internal array. Another reason
        // is that for the on-heap buffer, directly operating the underlying array can reduce
        // additional overhead compared to generating a NIO buffer.
        //如果缓冲区位于堆上，则直接操作底层数组。
        // 这里不直接使用NIO buffer的原因主要有两个：
        // 一是有些压缩库会使用底层数组来做堆buffer，但我们的输入buffer可能是只读的ByteBuffer，访问内部数组是非法的。
        //另一个原因是，对于堆上缓冲区来说，与生成 NIO 缓冲区相比，直接操作底层数组可以减少额外的开销。
        if (!memorySegment.isOffHeap()) {
            return blockDecompressor.decompress(
                    memorySegment.getArray(),
                    buffer.getMemorySegmentOffset(),
                    length,
                    internalBufferArray,
                    0);
        } else {
            // decompress the given buffer into the internal heap buffer
            //将给定缓冲区解压缩到内部堆缓冲区
            return blockDecompressor.decompress(
                    buffer.getNioBuffer(0, length),
                    0,
                    length,
                    internalBuffer.getNioBuffer(0, internalBuffer.capacity()),
                    0);
        }
    }
}
