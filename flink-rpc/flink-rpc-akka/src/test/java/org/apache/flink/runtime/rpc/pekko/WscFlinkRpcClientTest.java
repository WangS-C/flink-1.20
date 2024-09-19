package org.apache.flink.runtime.rpc.pekko;

import org.apache.pekko.actor.ActorSystem;

import java.util.concurrent.CompletableFuture;

public class WscFlinkRpcClientTest {

        public static void main(String[] args) throws Exception {
            // 1. 创建RPC服务
            ActorSystem defaultActorSystem = PekkoUtils.createDefaultActorSystem();
            PekkoRpcService akkaRpcService = new PekkoRpcService(defaultActorSystem,
                    PekkoRpcServiceConfiguration.defaultConfiguration());

            // 2. 连接远程RPC服务，注意：连接地址是服务端程序打印的地址
            CompletableFuture<TaskGateway> gatewayFuture = akkaRpcService
                    .connect("pekko.tcp://flink@192.168.50.151:60972/user/rpc/dbc74082-bf04-4c1f-a94e-6892ba8f29af",
                            TaskGateway.class);

            // 3. 远程调用
            TaskGateway gateway = gatewayFuture.get();
            System.out.println(gateway.sayHello("flink-pekko"));
        }
    }
