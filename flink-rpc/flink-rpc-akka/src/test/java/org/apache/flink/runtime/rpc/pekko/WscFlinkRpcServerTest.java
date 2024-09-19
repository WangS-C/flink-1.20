package org.apache.flink.runtime.rpc.pekko;

import org.apache.pekko.actor.ActorSystem;

public class WscFlinkRpcServerTest {
    public static void main(String[] args) throws Exception {
        // 1. 创建RPC服务
        ActorSystem defaultActorSystem = PekkoUtils.createDefaultActorSystem();
        PekkoRpcService akkaRpcService = new PekkoRpcService(
                defaultActorSystem,
                PekkoRpcServiceConfiguration.defaultConfiguration());

        // 2. 创建TaskEndpoint实例
        TaskEndpoint endpoint = new TaskEndpoint(akkaRpcService);
        System.out.println("address : " + endpoint.getAddress());

        // 3. 启动Endpoint
        endpoint.start();
    }
}
