package org.apache.flink.runtime.rpc.pekko;

import org.apache.flink.runtime.rpc.RpcGateway;

public interface TaskGateway extends RpcGateway {
        String sayHello(String name);
    }
