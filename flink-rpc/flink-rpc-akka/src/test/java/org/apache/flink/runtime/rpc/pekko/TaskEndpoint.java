package org.apache.flink.runtime.rpc.pekko;

import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;

public class TaskEndpoint extends RpcEndpoint implements TaskGateway {

        public TaskEndpoint(RpcService rpcService) {
            super(rpcService);
        }

        @Override
        public String sayHello(String name) {
            return "hello " + name;
        }

    }
