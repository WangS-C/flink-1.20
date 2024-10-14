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

package org.apache.flink.runtime.resourcemanager.active;

import org.apache.flink.runtime.blocklist.BlockedNodeRetriever;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.types.ResourceIDRetrievable;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * A {@link ResourceManagerDriver} is responsible for requesting and releasing resources from/to a
 * particular external resource manager.
 */
public interface ResourceManagerDriver<WorkerType extends ResourceIDRetrievable> {

    /**
     * Initialize the deployment specific components.
     *
     * @param resourceEventHandler Handler that handles resource events.
     * @param mainThreadExecutor Rpc main thread executor.
     * @param ioExecutor IO executor.
     * @param blockedNodeRetriever To retrieve all blocked nodes
     */
    void initialize(
            ResourceEventHandler<WorkerType> resourceEventHandler,
            ScheduledExecutor mainThreadExecutor,
            Executor ioExecutor,
            BlockedNodeRetriever blockedNodeRetriever) throws Exception;

    /** Terminate the deployment specific components. */
    void terminate() throws Exception;

    /**
     * The deployment specific code to deregister the application. This should report the
     * application's final status.
     *
     * <p>This method also needs to make sure all pending containers that are not registered yet are
     * returned.
     *
     * @param finalStatus The application status to report.
     * @param optionalDiagnostics A diagnostics message or {@code null}.
     *
     * @throws Exception if the application could not be deregistered.
     */
    void deregisterApplication(
            ApplicationStatus finalStatus,
            @Nullable String optionalDiagnostics) throws Exception;

    /**
     * Request resource from the external resource manager.
     *
     * <p>This method request a new resource from the external resource manager, and tries to launch
     * a task manager inside the allocated resource, with respect to the provided
     * taskExecutorProcessSpec. The returned future will be completed with a worker node in the
     * deployment specific type, or exceptionally if the allocation has failed.
     *
     * <p>Note: The returned future could be cancelled by ResourceManager. This means
     * ResourceManager don't need this resource anymore, Driver should try to cancel this request
     * from the external resource manager.
     *
     * <p>Note: Completion of the returned future does not necessarily mean the success of resource
     * allocation and task manager launching. Allocation and launching failures can still happen
     * after the future completion. In such cases, {@link ResourceEventHandler#onWorkerTerminated}
     * will be called.
     *
     * <p>The future is guaranteed to be completed in the rpc main thread, before trying to launch
     * the task manager, thus before the task manager registration. It is also guaranteed that
     * {@link ResourceEventHandler#onWorkerTerminated} will not be called on the requested worker,
     * until the returned future is completed successfully.
     *
     * @param taskExecutorProcessSpec Resource specification of the requested worker.
     *
     * @return Future that wraps worker node of the requested resource, in the deployment specific
     *         type.
     */
    //向外部资源管理器请求资源。
    //此方法从外部资源管理器请求新资源，并尝试根据提供的 taskExecutorProcessSpec 在分配的资源内启动任务管理器。
    //返回的 future 将使用部署特定类型的工作节点完成，或者在分配失败的情况下例外。
    //注意：返回的 future 可以被 ResourceManager 取消。
    //这意味着ResourceManager不再需要这个资源，Driver应该尝试从外部资源管理器取消这个请求。
    //注意：返回的 future 完成并不一定意味着资源分配和任务管理器启动成功。
    // 未来完成后，分配和启动失败仍然可能发生。在这种情况下，将调用ResourceEventHandler. onWorkerTerminated 。
    //未来保证在 rpc 主线程中完成，在尝试启动任务管理器之前，即在任务管理器注册之前。
    // 它还保证不会在所请求的工作线程上调用ResourceEventHandler. onWorkerTerminated ，直到返回的 future 成功完成。
    //参数：
    //taskExecutorProcessSpec – 所请求的工作线程的资源规范。
    //返回：
    //以部署特定类型包装所请求资源的工作节点的 Future。
    CompletableFuture<WorkerType> requestResource(TaskExecutorProcessSpec taskExecutorProcessSpec);

    /**
     * Release resource to the external resource manager.
     *
     * @param worker Worker node to be released, in the deployment specific type.
     */
    void releaseResource(WorkerType worker);
}
