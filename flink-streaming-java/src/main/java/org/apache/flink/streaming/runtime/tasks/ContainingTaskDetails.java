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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.execution.Environment;

/** Details about the operator containing task (such as {@link StreamTask}). */
//有关包含任务的运算符（例如StreamTask ）的详细信息
@Internal
public interface ContainingTaskDetails extends EnvironmentProvider {

    default ClassLoader getUserCodeClassLoader() {
        return getEnvironment().getUserCodeClassLoader().asClassLoader();
    }

    default ExecutionConfig getExecutionConfig() {
        return this.getEnvironment().getExecutionConfig();
    }

    default Configuration getJobConfiguration() {
        return this.getEnvironment().getJobConfiguration();
    }

    default int getIndexInSubtaskGroup() {
        return this.getEnvironment().getTaskInfo().getIndexOfThisSubtask();
    }
}

interface EnvironmentProvider {
    Environment getEnvironment();
}
