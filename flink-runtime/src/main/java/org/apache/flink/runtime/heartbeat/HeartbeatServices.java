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

package org.apache.flink.runtime.heartbeat;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HeartbeatManagerOptions;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;

/**
 * HeartbeatServices gives access to all services needed for heartbeating. This includes the
 * creation of heartbeat receivers and heartbeat senders.
 */
public interface HeartbeatServices {

    /**
     * Creates a heartbeat manager which does not actively send heartbeats.
     *
     * @param resourceId Resource Id which identifies the owner of the heartbeat manager
     * @param heartbeatListener Listener which will be notified upon heartbeat timeouts for
     *     registered targets
     * @param mainThreadExecutor Scheduled executor to be used for scheduling heartbeat timeouts
     * @param log Logger to be used for the logging
     * @param <I> Type of the incoming payload
     * @param <O> Type of the outgoing payload
     * @return A new HeartbeatManager instance
     */
    <I, O> HeartbeatManager<I, O> createHeartbeatManager(
            ResourceID resourceId,
            HeartbeatListener<I, O> heartbeatListener,
            ScheduledExecutor mainThreadExecutor,
            Logger log);

    /**
     * Creates a heartbeat manager which actively sends heartbeats to monitoring targets.
     *
     * @param resourceId Resource Id which identifies the owner of the heartbeat manager
     * @param heartbeatListener Listener which will be notified upon heartbeat timeouts for
     *     registered targets
     * @param mainThreadExecutor Scheduled executor to be used for scheduling heartbeat timeouts and
     *     periodically send heartbeat requests
     * @param log Logger to be used for the logging
     * @param <I> Type of the incoming payload
     * @param <O> Type of the outgoing payload
     * @return A new HeartbeatManager instance which actively sends heartbeats
     */
    <I, O> HeartbeatManager<I, O> createHeartbeatManagerSender(
            ResourceID resourceId,
            HeartbeatListener<I, O> heartbeatListener,
            ScheduledExecutor mainThreadExecutor,
            Logger log);

    /**
     * Creates an HeartbeatServices instance from a {@link Configuration}.
     *
     * @param configuration Configuration to be used for the HeartbeatServices creation
     * @return An HeartbeatServices instance created from the given configuration
     */
    //从Configuration创建 HeartbeatServices 实例。
    //参数：
    //configuration – 用于创建 HeartbeatServices 的配置
    //返回：
    //根据给定配置创建的 HeartbeatServices 实例
    static HeartbeatServices fromConfiguration(Configuration configuration) {
        long heartbeatInterval =
                configuration.get(HeartbeatManagerOptions.HEARTBEAT_INTERVAL).toMillis();

        long heartbeatTimeout =
                configuration.get(HeartbeatManagerOptions.HEARTBEAT_TIMEOUT).toMillis();

        int failedRpcRequestsUntilUnreachable =
                configuration.get(HeartbeatManagerOptions.HEARTBEAT_RPC_FAILURE_THRESHOLD);

        return new HeartbeatServicesImpl(
                heartbeatInterval, heartbeatTimeout, failedRpcRequestsUntilUnreachable);
    }

    static HeartbeatServices noOp() {
        return NoOpHeartbeatServices.getInstance();
    }
}
