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

package org.apache.flink.runtime.jobgraph;

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An intermediate data set is the data set produced by an operator - either a source or any
 * intermediate operation.
 *
 * <p>Intermediate data sets may be read by other operators, materialized, or discarded.
 */
//中间数据集是由操作符(源或任何中间操作) 产生的数据集。
//中间数据集可以被其他操作符读取、具体化或丢弃。
public class IntermediateDataSet implements java.io.Serializable {

    private static final long serialVersionUID = 1L;

    // 标识符
    private final IntermediateDataSetID id; // the identifier

    //产生这个数据集的操作
    private final JobVertex producer; // the operation that produced this data set

    // All consumers must have the same partitioner and parallelism
    // 所有消费者必须具有相同的分区器和并行性
    private final List<JobEdge> consumers = new ArrayList<>();

    // The type of partition to use at runtime
    //运行时要使用的分区类型
    private final ResultPartitionType resultType;

    // 分布模式
    private DistributionPattern distributionPattern;

    private boolean isBroadcast;

    // --------------------------------------------------------------------------------------------

    public IntermediateDataSet(
            IntermediateDataSetID id, ResultPartitionType resultType, JobVertex producer) {
        this.id = checkNotNull(id);
        this.producer = checkNotNull(producer);
        this.resultType = checkNotNull(resultType);
    }

    // --------------------------------------------------------------------------------------------

    public IntermediateDataSetID getId() {
        return id;
    }

    public JobVertex getProducer() {
        return producer;
    }

    public List<JobEdge> getConsumers() {
        return this.consumers;
    }

    public boolean isBroadcast() {
        return isBroadcast;
    }

    public DistributionPattern getDistributionPattern() {
        return distributionPattern;
    }

    public ResultPartitionType getResultType() {
        return resultType;
    }

    // --------------------------------------------------------------------------------------------

    public void addConsumer(JobEdge edge) {
        // sanity check
        checkState(id.equals(edge.getSourceId()), "Incompatible dataset id.");

        if (consumers.isEmpty()) {
            distributionPattern = edge.getDistributionPattern();
            isBroadcast = edge.isBroadcast();
        } else {
            checkState(
                    distributionPattern == edge.getDistributionPattern(),
                    "Incompatible distribution pattern.");
            checkState(isBroadcast == edge.isBroadcast(), "Incompatible broadcast type.");
        }
        consumers.add(edge);
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public String toString() {
        return "Intermediate Data Set (" + id + ")";
    }
}
