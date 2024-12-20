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

package org.apache.flink.runtime.io.network.partition;

import java.io.IOException;
import java.util.Optional;

/** Interface for creating result partitions. */
public interface ResultPartitionProvider {

    /** Returns the requested intermediate result partition input view. */
    ResultSubpartitionView createSubpartitionView(
            ResultPartitionID partitionId,
            ResultSubpartitionIndexSet indexSet,
            BufferAvailabilityListener availabilityListener)
            throws IOException;

    /**
     * If the upstream task's partition has been registered, returns the result subpartition input
     * view immediately, otherwise register the listener and return empty.
     *
     * @param partitionId the result partition id
     * @param indexSet the index set
     * @param availabilityListener the buffer availability listener
     * @param partitionRequestListener the partition request listener
     * @return the result subpartition view
     * @throws IOException the thrown exception
     */
    //如果上游任务的分区已注册，则立即返回结果子分区输入视图，否则注册侦听器并返回空。
    //参数:
    //partitionId -结果分区id
    //indexSet -索引集
    //availabilityListener -缓冲区可用性侦听器
    //partitionRequestListener -分区请求侦听器
    Optional<ResultSubpartitionView> createSubpartitionViewOrRegisterListener(
            ResultPartitionID partitionId,
            ResultSubpartitionIndexSet indexSet,
            BufferAvailabilityListener availabilityListener,
            PartitionRequestListener partitionRequestListener)
            throws IOException;

    /**
     * Release the given listener in this result partition provider.
     *
     * @param listener the given listener
     */
    void releasePartitionRequestListener(PartitionRequestListener listener);
}
