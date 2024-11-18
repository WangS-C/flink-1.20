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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.PartitionRequestClient;
import org.apache.flink.runtime.io.network.TaskEventPublisher;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class NettyConnectionManager implements ConnectionManager {

    private final NettyServer server;

    private final NettyClient client;

    private final NettyBufferPool bufferPool;

    private final PartitionRequestClientFactory partitionRequestClientFactory;

    private final NettyProtocol nettyProtocol;

    public NettyConnectionManager(
            ResultPartitionProvider partitionProvider,
            TaskEventPublisher taskEventPublisher,
            NettyConfig nettyConfig,
            int maxNumberOfConnections,
            boolean connectionReuseEnabled) {

        this(
                //创建NettyBufferPool
                new NettyBufferPool(nettyConfig.getNumberOfArenas()),
                partitionProvider,
                taskEventPublisher,
                nettyConfig,
                maxNumberOfConnections,
                connectionReuseEnabled);
    }

    @VisibleForTesting
    public NettyConnectionManager(
            NettyBufferPool bufferPool,
            ResultPartitionProvider partitionProvider,
            TaskEventPublisher taskEventPublisher,
            NettyConfig nettyConfig,
            int maxNumberOfConnections,
            boolean connectionReuseEnabled) {

        //构建TaskManager的Netty服务端
        this.server = new NettyServer(nettyConfig);
        //构建TaskManager的Netty客户端
        this.client = new NettyClient(nettyConfig);
        this.bufferPool = checkNotNull(bufferPool);

        this.partitionRequestClientFactory =
                new PartitionRequestClientFactory(
                        client,
                        nettyConfig.getNetworkRetries(),
                        maxNumberOfConnections,
                        connectionReuseEnabled);

        //构建NettyProtocol
        this.nettyProtocol =
                new NettyProtocol(
                        checkNotNull(partitionProvider), checkNotNull(taskEventPublisher));
    }

    //简述下Netty Bootstrap启动过程涉及到的8个基本步骤。
    //1)、设置EventLoopGroup线程组。
    //2)、设置Channel通道类型。
    //3)、设置监听端口。
    //4)、设置通道参数。
    //5)、装配Handler流水线。
    //6)、启动、端口绑定
    //7)、等待通道关闭。
    //8)、关闭EventLoopGroup线程组。
    @Override
    public int start() throws IOException {
        //客户端初始化，
        //客户端初始化阶段比较简单，只是新建一个bootstrap引导实例并设置一些连接参数，
        //在当前阶段Task并未启动也不知道该连哪个节点，也未装配Netty重要的Handler流水线信息。
        client.init(nettyProtocol, bufferPool);

        //服务端初始化，
        //在TaskManager启动过程中，NettyServer初始化过程比较完整。
        //创建服务端bootstrap引导类实例，添加ChannelHandlers流水线信息，最后启动了server服务。
        return server.init(nettyProtocol, bufferPool);
    }

    @Override
    public PartitionRequestClient createPartitionRequestClient(ConnectionID connectionId)
            throws IOException, InterruptedException {
        return partitionRequestClientFactory.createPartitionRequestClient(connectionId);
    }

    @Override
    public void closeOpenChannelConnections(ConnectionID connectionId) {
        partitionRequestClientFactory.closeOpenChannelConnections(connectionId);
    }

    @Override
    public int getNumberOfActiveConnections() {
        return partitionRequestClientFactory.getNumberOfActiveClients();
    }

    @Override
    public void shutdown() {
        client.shutdown();
        server.shutdown();
    }

    NettyClient getClient() {
        return client;
    }

    NettyServer getServer() {
        return server;
    }

    NettyBufferPool getBufferPool() {
        return bufferPool;
    }
}
