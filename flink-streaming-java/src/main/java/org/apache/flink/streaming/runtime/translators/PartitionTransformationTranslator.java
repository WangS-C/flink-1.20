/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.translators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.graph.SimpleTransformationTranslator;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.TransformationTranslator;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.StreamExchangeMode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link TransformationTranslator} for the {@link PartitionTransformation}.
 *
 * @param <OUT> The type of the elements that result from the {@code PartitionTransformation} being
 *     translated.
 */
@Internal
public class PartitionTransformationTranslator<OUT>
        extends SimpleTransformationTranslator<OUT, PartitionTransformation<OUT>> {

    @Override
    protected Collection<Integer> translateForBatchInternal(
            final PartitionTransformation<OUT> transformation, final Context context) {
        return translateInternal(transformation, context, true);
    }

    @Override
    protected Collection<Integer> translateForStreamingInternal(
            final PartitionTransformation<OUT> transformation, final Context context) {
        return translateInternal(transformation, context, false);
    }

    private Collection<Integer> translateInternal(
            final PartitionTransformation<OUT> transformation,
            final Context context,
            boolean supportsBatchExchange) {
        checkNotNull(transformation);
        checkNotNull(context);

        final StreamGraph streamGraph = context.getStreamGraph();

        final List<Transformation<?>> parentTransformations = transformation.getInputs();
        checkState(
                parentTransformations.size() == 1,
                "Expected exactly one input transformation but found "
                        + parentTransformations.size());
        final Transformation<?> input = parentTransformations.get(0);

        List<Integer> resultIds = new ArrayList<>();

        StreamExchangeMode exchangeMode = transformation.getExchangeMode();
        // StreamExchangeMode#BATCH has no effect in streaming mode so we can safely reset it to
        // UNDEFINED and let Flink decide on the best exchange mode.
        if (!supportsBatchExchange && exchangeMode == StreamExchangeMode.BATCH) {
            exchangeMode = StreamExchangeMode.UNDEFINED;
        }

        for (Integer inputId : context.getStreamNodeIds(input)) {
            final int virtualId = Transformation.getNewNodeId();
            //添加虚拟的 Partition 节点
            streamGraph.addVirtualPartitionNode(
                    inputId, virtualId, transformation.getPartitioner(), exchangeMode);
            resultIds.add(virtualId);
        }
        return resultIds;
    }
}
