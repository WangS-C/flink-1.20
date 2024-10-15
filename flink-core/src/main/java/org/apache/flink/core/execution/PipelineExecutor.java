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

package org.apache.flink.core.execution;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.configuration.Configuration;

import java.util.concurrent.CompletableFuture;

/** The entity responsible for executing a {@link Pipeline}, i.e. a user job. */
@Internal
public interface PipelineExecutor {

    /**
     * Executes a {@link Pipeline} based on the provided configuration and returns a {@link
     * JobClient} which allows to interact with the job being executed, e.g. cancel it or take a
     * savepoint.
     *
     * <p><b>ATTENTION:</b> The caller is responsible for managing the lifecycle of the returned
     * {@link JobClient}. This means that e.g. {@code close()} should be called explicitly at the
     * call-site.
     *
     * @param pipeline the {@link Pipeline} to execute
     * @param configuration the {@link Configuration} with the required execution parameters
     * @param userCodeClassloader the {@link ClassLoader} to deserialize usercode
     * @return a {@link CompletableFuture} with the {@link JobClient} corresponding to the pipeline.
     */
    //根据提供的配置执行Pipeline并返回一个JobClient ，它允许与正在执行的作业进行交互，例如取消它或获取保存点。
    //注意：调用者负责管理返回的JobClient的生命周期。这意味着应该在调用站点显式调用close() 。
    //参数：
    //pipeline要执行的Pipeline
    //configuration – 具有所需执行参数的Configuration
    //userCodeClassloader – 反序列化用户代码的ClassLoader
    //返回：
    //具有与管道对应的JobClient的CompletableFuture 。
    CompletableFuture<JobClient> execute(
            final Pipeline pipeline,
            final Configuration configuration,
            final ClassLoader userCodeClassloader)
            throws Exception;
}
