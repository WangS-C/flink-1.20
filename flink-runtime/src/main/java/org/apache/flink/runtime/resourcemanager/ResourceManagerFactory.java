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

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceIDRetrievable;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.groups.ResourceManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.SlotManagerMetricGroup;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.security.token.DelegationTokenManager;
import org.apache.flink.util.ConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.UUID;
import java.util.concurrent.Executor;

/**
 * {@link ResourceManager} factory.
 *
 * @param <T> type of the workers of the ResourceManager
 */
//ResourceManager工厂
public abstract class ResourceManagerFactory<T extends ResourceIDRetrievable> {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    public ResourceManagerProcessContext createResourceManagerProcessContext(
            Configuration configuration,
            ResourceID resourceId,
            RpcService rpcService,
            HighAvailabilityServices highAvailabilityServices,
            HeartbeatServices heartbeatServices,
            DelegationTokenManager delegationTokenManager,
            FatalErrorHandler fatalErrorHandler,
            ClusterInformation clusterInformation,
            @Nullable String webInterfaceUrl,
            MetricRegistry metricRegistry,
            String hostname,
            Executor ioExecutor)
            throws ConfigurationException {

        final Configuration runtimeServicesAndRmConfig =
                getEffectiveConfigurationForResourceManagerAndRuntimeServices(configuration);

        final ResourceManagerRuntimeServicesConfiguration runtimeServiceConfig =
                createResourceManagerRuntimeServicesConfiguration(runtimeServicesAndRmConfig);

        final Configuration rmConfig =
                getEffectiveConfigurationForResourceManager(runtimeServicesAndRmConfig);

        return new ResourceManagerProcessContext(
                rmConfig,
                resourceId,
                runtimeServiceConfig,
                rpcService,
                highAvailabilityServices,
                heartbeatServices,
                delegationTokenManager,
                fatalErrorHandler,
                clusterInformation,
                webInterfaceUrl,
                metricRegistry,
                hostname,
                ioExecutor);
    }

    public ResourceManager<T> createResourceManager(
            ResourceManagerProcessContext context, UUID leaderSessionId) throws Exception {

        final ResourceManagerRuntimeServices resourceManagerRuntimeServices =
                //创建资源管理器运行时服务 创建了SlotManager  和 JobLeaderIdService
                createResourceManagerRuntimeServices(
                        context.getRmRuntimeServicesConfig(),
                        context.getRpcService(),
                        context.getHighAvailabilityServices(),
                        SlotManagerMetricGroup.create(
                                context.getMetricRegistry(), context.getHostname()));

        //创建资源管理器
        return createResourceManager(
                context.getRmConfig(),
                context.getResourceId(),
                context.getRpcService(),
                leaderSessionId,
                context.getHeartbeatServices(),
                context.getDelegationTokenManager(),
                context.getFatalErrorHandler(),
                context.getClusterInformation(),
                context.getWebInterfaceUrl(),
                ResourceManagerMetricGroup.create(
                        context.getMetricRegistry(), context.getHostname()),
                resourceManagerRuntimeServices,
                context.getIoExecutor());
    }

    /** This indicates whether the process should be terminated after losing leadership. */
    //这表明进程在失去领导权后是否应该终止。
    protected boolean supportMultiLeaderSession() {
        return true;
    }

    /**
     * Configuration changes in this method will be visible to both {@link ResourceManager} and
     * {@link ResourceManagerRuntimeServices}. This can be overwritten by {@link
     * #getEffectiveConfigurationForResourceManager}.
     */
    //此方法中的配置更改对于ResourceManager和ResourceManagerRuntimeServices都是可见的。
    //这可以被getEffectiveConfigurationForResourceManager覆盖
    protected Configuration getEffectiveConfigurationForResourceManagerAndRuntimeServices(
            final Configuration configuration) {
        return configuration;
    }

    /**
     * Configuration changes in this method will be visible to only {@link ResourceManager}. This
     * can overwrite {@link #getEffectiveConfigurationForResourceManagerAndRuntimeServices}.
     */
    //此方法中的配置更改仅对ResourceManager可见。
    //这可以覆盖getEffectiveConfigurationForResourceManagerAndRuntimeServices 。
    protected Configuration getEffectiveConfigurationForResourceManager(
            final Configuration configuration) {
        return configuration;
    }

    protected abstract ResourceManager<T> createResourceManager(
            Configuration configuration,
            ResourceID resourceId,
            RpcService rpcService,
            UUID leaderSessionId,
            HeartbeatServices heartbeatServices,
            DelegationTokenManager delegationTokenManager,
            FatalErrorHandler fatalErrorHandler,
            ClusterInformation clusterInformation,
            @Nullable String webInterfaceUrl,
            ResourceManagerMetricGroup resourceManagerMetricGroup,
            ResourceManagerRuntimeServices resourceManagerRuntimeServices,
            Executor ioExecutor)
            throws Exception;

    private ResourceManagerRuntimeServices createResourceManagerRuntimeServices(
            ResourceManagerRuntimeServicesConfiguration rmRuntimeServicesConfig,
            RpcService rpcService,
            HighAvailabilityServices highAvailabilityServices,
            SlotManagerMetricGroup slotManagerMetricGroup) {

        return ResourceManagerRuntimeServices.fromConfiguration(
                rmRuntimeServicesConfig,
                highAvailabilityServices,
                rpcService.getScheduledExecutor(),
                slotManagerMetricGroup);
    }

    protected abstract ResourceManagerRuntimeServicesConfiguration
            createResourceManagerRuntimeServicesConfiguration(Configuration configuration)
                    throws ConfigurationException;
}
