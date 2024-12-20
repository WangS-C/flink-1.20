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

package org.apache.flink.yarn.entrypoint;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.resourcemanager.ResourceManagerRuntimeServicesConfiguration;
import org.apache.flink.runtime.resourcemanager.active.ActiveResourceManager;
import org.apache.flink.runtime.resourcemanager.active.ActiveResourceManagerFactory;
import org.apache.flink.runtime.resourcemanager.active.ResourceManagerDriver;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.yarn.DefaultYarnNodeManagerClientFactory;
import org.apache.flink.yarn.DefaultYarnResourceManagerClientFactory;
import org.apache.flink.yarn.YarnResourceManagerDriver;
import org.apache.flink.yarn.YarnWorkerNode;
import org.apache.flink.yarn.configuration.YarnResourceManagerDriverConfiguration;

/**
 * {@link ActiveResourceManagerFactory} implementation which creates a {@link ActiveResourceManager}
 * with {@link YarnResourceManagerDriver}.
 */
//ActiveResourceManagerFactory实现使用YarnResourceManagerDriver创建ActiveResourceManager 。
public class YarnResourceManagerFactory extends ActiveResourceManagerFactory<YarnWorkerNode> {

    private static final YarnResourceManagerFactory INSTANCE = new YarnResourceManagerFactory();

    private YarnResourceManagerFactory() {}

    public static YarnResourceManagerFactory getInstance() {
        return INSTANCE;
    }

    //创建ResourceManagerDriver
    @Override
    protected ResourceManagerDriver<YarnWorkerNode> createResourceManagerDriver(
            Configuration configuration, String webInterfaceUrl, String rpcAddress) {
        final YarnResourceManagerDriverConfiguration yarnResourceManagerDriverConfiguration =
                new YarnResourceManagerDriverConfiguration(
                        System.getenv(), rpcAddress, webInterfaceUrl);

        return new YarnResourceManagerDriver(
                configuration,
                yarnResourceManagerDriverConfiguration,
                DefaultYarnResourceManagerClientFactory.getInstance(),
                DefaultYarnNodeManagerClientFactory.getInstance());
    }

    @Override
    protected ResourceManagerRuntimeServicesConfiguration
            createResourceManagerRuntimeServicesConfiguration(Configuration configuration)
                    throws ConfigurationException {
        return ResourceManagerRuntimeServicesConfiguration.fromConfiguration(
                configuration, YarnWorkerResourceSpecFactory.INSTANCE);
    }

    @Override
    public boolean supportMultiLeaderSession() {
        // Multiple leader session is not supported by the Yarn deployment, because Flink RM relies
        // on the registration response from Yarn RM for recovering previous resources, but Yarn
        // only allows each AM process to register for once.
        //Yarn 部署不支持多个 Leader 会话，因为 Flink RM 依赖于
        //Yarn RM 的注册响应来恢复之前的资源，但 Yarn 只允许每个 AM 进程注册一次。
        return false;
    }
}
