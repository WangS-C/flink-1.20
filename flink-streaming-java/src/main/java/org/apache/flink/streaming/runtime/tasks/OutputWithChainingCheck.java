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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

/**
 * This is a wrapper for outputs to check whether the collected record has been emitted to a
 * downstream subtask or to a chained operator.
 */
//这是输出的包装器，用于检查收集的记录是否已发送到下游子任务或链式运算符。
@Internal
public interface OutputWithChainingCheck<OUT> extends WatermarkGaugeExposingOutput<OUT> {
    /**
     * @return true if the collected record has been emitted to a downstream subtask. Otherwise,
     *     false.
     */
    //返回：
    //如果收集的记录已发送到下游子任务，则为 true。否则为假。
    boolean collectAndCheckIfChained(OUT record);

    /**
     * @return true if the collected record has been emitted to a downstream subtask. Otherwise,
     *     false.
     */
    //返回：
    //如果收集的记录已发送到下游子任务，则为 true。否则为假。
    <X> boolean collectAndCheckIfChained(OutputTag<X> outputTag, StreamRecord<X> record);
}
