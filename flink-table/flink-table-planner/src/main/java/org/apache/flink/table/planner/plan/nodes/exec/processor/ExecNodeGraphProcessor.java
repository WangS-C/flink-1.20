/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.nodes.exec.processor;

import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;

/**
 * {@link ExecNodeGraphProcessor} plugin, use it can change or update the given {@link
 * ExecNodeGraph}.
 */
public interface ExecNodeGraphProcessor {

    /** Given an {@link ExecNodeGraph}, process it and return the result {@link ExecNodeGraph}. */
    //给定execnodeggraph ，处理它并返回结果execnodeggraph。
    ExecNodeGraph process(ExecNodeGraph execGraph, ProcessorContext context);
}
