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

package org.apache.flink.runtime.shuffle;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;

import java.io.Serializable;
import java.util.Collection;
import java.util.Optional;

/**
 * Interface for shuffle deployment descriptor of result partition resource.
 *
 * <p>The descriptor is used for the deployment of the partition producer/consumer and their data
 * exchange
 */
//结果分区资源的随机部署描述符接口。
//描述符用于分区生产者/ 消费者的部署及其数据交换
public interface ShuffleDescriptor extends Serializable {

    ResultPartitionID getResultPartitionID();

    /**
     * Returns whether the partition is known and registered with the {@link ShuffleMaster}
     * implementation.
     *
     * <p>When a partition consumer is being scheduled, it can happen that the producer of the
     * partition (consumer input channel) has not been scheduled and its location and other relevant
     * data is yet to be defined. To proceed with the consumer deployment, currently unknown input
     * channels have to be marked with placeholders. The placeholder is a special implementation of
     * the shuffle descriptor: {@link UnknownShuffleDescriptor}.
     *
     * <p>Note: this method is not supposed to be overridden in concrete shuffle implementation. The
     * only class where it returns {@code true} is {@link UnknownShuffleDescriptor}.
     *
     * @return whether the partition producer has been ever deployed and the corresponding shuffle
     *     descriptor is obtained from the {@link ShuffleMaster} implementation.
     */
    //返回分区是否已知并已向ShuffleMaster实现注册。
    //当分区消费者被调度时，可能会发生分区的生产者（消费者输入通道）尚未被调度并且其位置和其他相关数据尚未定义。
    // 要继续进行消费者部署，必须用占位符标记当前未知的输入通道。
    // 占位符是洗牌描述符的特殊实现： UnknownShuffleDescriptor 。
    //注意：在具体的 shuffle 实现中不应重写此方法。唯一返回true的类是UnknownShuffleDescriptor 。
    //返回：
    //分区生产者是否已经部署，并且从ShuffleMaster实现中获取相应的shuffle描述符。
    default boolean isUnknown() {
        return false;
    }

    /**
     * Returns the location of the producing task executor if the partition occupies local resources
     * there.
     *
     * <p>Indicates that this partition occupies local resources in the producing task executor.
     * Such partition requires that the task executor is running and being connected to be able to
     * consume the produced data. This is mostly relevant for the batch jobs and blocking result
     * partitions which can outlive the producer lifetime and be released externally. {@link
     * ShuffleEnvironment#releasePartitionsLocally(Collection)} can be used to release such kind of
     * partitions locally.
     *
     * @return the resource id of the producing task executor if the partition occupies local
     *     resources there
     */
    //如果分区占用了生产任务执行器的本地资源，则返回该位置。
    //表示该分区占用生产任务执行器本地资源。这种分区要求任务执行器正在运行并已连接才能使用生成的数据。
    // 这主要与批处理作业和阻塞结果分区相关，这些作业和阻塞结果分区可以比生产者的生命周期长并被外部释放。
    // ShuffleEnvironment. releasePartitionsLocally(Collection)可用于在本地释放此类分区。
    //返回：
    //如果分区占用了生产任务执行器的本地资源，则为生产任务执行器的资源ID
    Optional<ResourceID> storesLocalResourcesOn();
}
