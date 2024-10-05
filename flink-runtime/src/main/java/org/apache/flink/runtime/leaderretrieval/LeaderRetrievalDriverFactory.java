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

package org.apache.flink.runtime.leaderretrieval;

import org.apache.flink.runtime.rpc.FatalErrorHandler;

/** Factory for creating {@link LeaderRetrievalDriver} with different implementation. */
public interface LeaderRetrievalDriverFactory {

    /**
     * Create a specific {@link LeaderRetrievalDriver} and start the necessary services. For
     * example, NodeCache in Zookeeper, ConfigMap watcher in Kubernetes. They could get the leader
     * information change events and need to notify the leader listener by {@link
     * LeaderRetrievalEventHandler}.
     *
     * @param leaderEventHandler handler for the leader retrieval driver to notify leader change
     *     events.
     * @param fatalErrorHandler fatal error handler
     * @throws Exception when create a specific {@link LeaderRetrievalDriver} implementation and
     *     start the necessary services.
     */
    //创建特定的LeaderRetrievalDriver并启动必要的服务。
    // 例如Zookeeper中的NodeCache、Kubernetes中的ConfigMap watcher。
    // 他们可以获取领导者信息更改事件，并需要通过LeaderRetrievalEventHandler通知领导者侦听器。
    //参数：
    //leaderEventHandler – 领导者检索驱动程序的处理程序，用于通知领导者更改事件。
    //fatalErrorHandler – 致命错误处理程序
    LeaderRetrievalDriver createLeaderRetrievalDriver(
            LeaderRetrievalEventHandler leaderEventHandler, FatalErrorHandler fatalErrorHandler)
            throws Exception;
}
