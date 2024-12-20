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

import java.io.IOException;

/**
 * A {@link StreamTaskInput} used during recovery of in-flight data. It's converted to a more
 * efficient {@link StreamTaskInput} after recovery finished.
 */
//在恢复运行中数据期间使用的StreamTaskInput 。恢复完成后，它会转换为更高效的StreamTaskInput
public interface RecoverableStreamTaskInput<T> extends StreamTaskInput<T> {
    StreamTaskInput<T> finishRecovery() throws IOException;
}
