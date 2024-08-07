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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;


/** Tests for {@link StreamExecutionEnvironment}. */
class StreamExecutionEnvironmentTest2 {
    @Test
    void testParallelismBounds() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SourceFunction<Integer> srcFun = new SourceFunction<Integer>() {
            private static final long serialVersionUID = 1L;

            @Override
            public void run(SourceContext<Integer> ctx) throws Exception {
            }

            @Override
            public void cancel() {
            }
        };

        SingleOutputStreamOperator<Object> operator = env
                .addSource(srcFun)
                .map(line -> line)
                .cache()
                .flatMap(new FlatMapFunction<Integer, Object>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public void flatMap(Integer value, Collector<Object> out) throws Exception {
                    }
                });

        // default value for max parallelism
        assertThat(operator.getTransformation().getMaxParallelism()).isEqualTo(-1);

        // bounds for parallelism 1
        assertThatThrownBy(() -> operator.setParallelism(0)).isInstanceOf(IllegalArgumentException.class);

        // bounds for parallelism 2
        operator.setParallelism(1);
        assertThat(operator.getParallelism()).isOne();

        // bounds for parallelism 3
        operator.setParallelism(1 << 15);
        assertThat(operator.getParallelism()).isEqualTo(1 << 15);

        // default value after generating
        env.getStreamGraph(false).getJobGraph();
        assertThat(operator.getTransformation().getMaxParallelism()).isEqualTo(-1);

        // configured value after generating
        env.setMaxParallelism(42);
        env.getStreamGraph(false).getJobGraph();
        assertThat(operator.getTransformation().getMaxParallelism()).isEqualTo(42);

        // bounds configured parallelism 1
        assertThatThrownBy(() -> env.setMaxParallelism(0)).isInstanceOf(IllegalArgumentException.class);

        // bounds configured parallelism 2
        assertThatThrownBy(() -> env.setMaxParallelism(1 + (1 << 15))).isInstanceOf(
                IllegalArgumentException.class);

        // bounds for max parallelism 1
        assertThatThrownBy(() -> operator.setMaxParallelism(0)).isInstanceOf(
                IllegalArgumentException.class);

        // bounds for max parallelism 2
        assertThatThrownBy(() -> operator.setMaxParallelism(1 + (1 << 15))).isInstanceOf(
                IllegalArgumentException.class);

        // bounds for max parallelism 3
        operator.setMaxParallelism(1);
        assertThat(operator.getTransformation().getMaxParallelism()).isOne();

        // bounds for max parallelism 4
        operator.setMaxParallelism(1 << 15);
        assertThat(operator.getTransformation().getMaxParallelism()).isEqualTo(1 << 15);

        // override config
        env.getStreamGraph(false).getJobGraph();
        assertThat(operator.getTransformation().getMaxParallelism()).isEqualTo(1 << 15);
    }


}
