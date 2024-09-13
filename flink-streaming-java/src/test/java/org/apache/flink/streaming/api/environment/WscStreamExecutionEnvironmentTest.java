/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.environment;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.executiongraph.DefaultExecutionGraph;
import org.apache.flink.runtime.executiongraph.TestingDefaultExecutionGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.jsonplan.JsonPlanGenerator;
import org.apache.flink.streaming.api.graph.StreamGraph;

import org.junit.jupiter.api.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;


/** Tests for {@link StreamExecutionEnvironment}. */
class WscStreamExecutionEnvironmentTest {
    @Test
    void testVirtualPartitionNodes() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.fromData(1, 2, 3, 4).map(line -> line + 1).shuffle().filter(line -> line > 0).print();

        StreamGraph streamGraph = env.getStreamGraph(false);
        System.out.println(">>>>>>>>>>>>>>>>>StreamGraph>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        System.out.println(streamGraph.getStreamingPlanAsJSON());
        JobGraph jobGraph = streamGraph.getJobGraph();
        System.out.println(">>>>>>>>>>>>>>>>>JobGraph>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        System.out.println(JsonPlanGenerator.generatePlan(jobGraph));

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);

        DefaultExecutionGraph executionGraph =
                TestingDefaultExecutionGraphBuilder.newBuilder()
                        .setJobGraph(jobGraph)
                        .setRpcTimeout( Time.seconds(10L))
                        .build(executor);

        System.out.println(">>>>>>>>>>>>>>>>>ExecutionGraph>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        System.out.println(executionGraph);
    }
}


