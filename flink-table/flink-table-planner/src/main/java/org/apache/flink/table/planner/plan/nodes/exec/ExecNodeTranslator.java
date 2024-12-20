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

package org.apache.flink.table.planner.plan.nodes.exec;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.table.delegation.Planner;

/**
 * An {@link ExecNodeTranslator} is responsible for translating an {@link ExecNode} to {@link
 * Transformation}s.
 *
 * @param <T> The type of the elements that result from this translator.
 */
@Internal
public interface ExecNodeTranslator<T> {

    /**
     * Translates this node into a {@link Transformation}.
     *
     * <p>NOTE: This method should return same translate result if called multiple times.
     *
     * @param planner The {@link Planner} of the translated graph.
     */
    //将此节点转换为Transformation 。
    //注意：如果多次调用此方法应返回相同的翻译结果。
    Transformation<T> translateToPlan(Planner planner);
}
