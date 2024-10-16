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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategy;

import java.util.List;

/** Component which is used by {@link SchedulingStrategy} to commit scheduling decisions. */
public interface SchedulerOperations {

    /**
     * Allocate slots and deploy the vertex when slots are returned. Vertices will be deployed only
     * after all of them have been assigned slots. The given order will be respected, i.e. tasks
     * with smaller indices will be deployed earlier. Only vertices in CREATED state will be
     * accepted. Errors will happen if scheduling Non-CREATED vertices.
     *
     * @param verticesToDeploy The execution vertices to deploy
     */
    //分配槽并在返回槽时部署顶点。只有在所有顶点都分配了槽位后才会部署顶点。将尊重给定的顺序，即索引较小的任务将更早部署。仅接受处于 CREATED 状态的顶点。如果调度非创建的顶点，将会发生错误。
    //参数：
    //verticesToDeploy – 要部署的执行顶点
    void allocateSlotsAndDeploy(List<ExecutionVertexID> verticesToDeploy);
}
