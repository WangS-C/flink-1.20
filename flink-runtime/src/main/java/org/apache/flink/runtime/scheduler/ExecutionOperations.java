/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.executiongraph.Execution;

import java.util.concurrent.CompletableFuture;

/** Operations on the {@link Execution}. */
public interface ExecutionOperations {

    /**
     * Deploy the execution.
     *
     * @param execution to deploy.
     * @throws JobException if the execution cannot be deployed to the assigned resource
     */
    //部署执行。
    //参数：
    //execution ——部署。
    void deploy(Execution execution) throws JobException;

    /**
     * Cancel the execution.
     *
     * @param execution to cancel
     * @return Future which completes when the cancellation is done
     */
    CompletableFuture<?> cancel(Execution execution);

    /**
     * Mark the execution as FAILED.
     *
     * @param execution to mark as failed.
     * @param cause of the execution failure
     */
    void markFailed(Execution execution, Throwable cause);
}
