/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;

/** Basic interface for inputs of stream operators. */
//流运算符输入的基本接口。
@Internal
public interface StreamTaskInput<T> extends PushingAsyncDataInput<T>, Closeable {
    int UNSPECIFIED = -1;

    /** Returns the input index of this input. */
    //返回此输入的输入索引
    int getInputIndex();

    /** Prepares to spill the in-flight input buffers as checkpoint snapshot. */
    //准备将运行中的输入缓冲区溢出作为检查点快照。
    CompletableFuture<Void> prepareSnapshot(
            ChannelStateWriter channelStateWriter, long checkpointId) throws CheckpointException;
}
