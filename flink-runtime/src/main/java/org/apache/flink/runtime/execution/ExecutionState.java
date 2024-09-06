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

package org.apache.flink.runtime.execution;

/**
 * An enumeration of all states that a task can be in during its execution. Tasks usually start in
 * the state {@code CREATED} and switch states according to this diagram:
 *
 * <pre>{@code
 *  CREATED  -> SCHEDULED -> DEPLOYING -> INITIALIZING -> RUNNING -> FINISHED
 *     |            |            |          |              |
 *     |            |            |    +-----+--------------+
 *     |            |            V    V
 *     |            |         CANCELLING -----+----> CANCELED
 *     |            |                         |
 *     |            +-------------------------+
 *     |
 *     |                                   ... -> FAILED
 *     V
 * RECONCILING  -> INITIALIZING | RUNNING | FINISHED | CANCELED | FAILED
 *
 * }</pre>
 *
 * <p>It is possible to enter the {@code RECONCILING} state from {@code CREATED} state if job
 * manager fail over, and the {@code RECONCILING} state can switch into any existing task state.
 *
 * <p>It is possible to enter the {@code FAILED} state from any other state.
 *
 * <p>The states {@code FINISHED}, {@code CANCELED}, and {@code FAILED} are considered terminal
 * states.
 */

/**
 * 任务在执行期间可以处于的所有状态的枚举。任务通常从已创建的状态开始，并根据此图表切换状态:
 * <pre>{@code
 *  已创建  -> 作业已经被调度系统接收 -> 正在部署 -> 正在初始化 -> 正在运行 -> 已完成
 *     |            |            |          |              |
 *     |            |            |    +-----+--------------+
 *     |            |            V    V
 *     |            |         取消 -----+----> 已取消
 *     |            |                         |
 *     |            +-------------------------+
 *     |
 *     |                                   ... -> 失败
 *     V
 * RECONCILING  -> 正在初始化 | 正在运行 | 已完成 | 已取消 | 失败
 *
 * }</pre>
 * 如果作业管理器故障转移，则可以从已创建状态进入协调状态，并且协调状态可以切换到任何现有任务状态。
 * 可以从任何其他状态进入失败状态。
 * “ 已完成 ” 、 “ 已取消” 和 “ 失败 ” 状态被视为终端状态。
 */
public enum ExecutionState {
    CREATED,

    SCHEDULED,

    DEPLOYING,

    RUNNING,

    /**
     * This state marks "successfully completed". It can only be reached when a program reaches the
     * "end of its input". The "end of input" can be reached when consuming a bounded input (fix set
     * of files, bounded query, etc) or when stopping a program (not cancelling!) which make the
     * input look like it reached its end at a specific point.
     */
    FINISHED,

    CANCELING,

    CANCELED,

    FAILED,

    RECONCILING,

    /** Restoring last possible valid state of the task if it has it. */
    INITIALIZING;

    public boolean isTerminal() {
        return this == FINISHED || this == CANCELED || this == FAILED;
    }
}
