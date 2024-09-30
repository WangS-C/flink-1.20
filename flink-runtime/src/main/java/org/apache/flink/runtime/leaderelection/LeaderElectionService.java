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

package org.apache.flink.runtime.leaderelection;

import java.util.UUID;

/**
 * Interface for a service which allows to elect a leader among a group of contenders.
 *
 * <p>Prior to using this service, it has to be started calling the start method. The start method
 * takes the contender as a parameter. If there are multiple contenders, then each contender has to
 * instantiate its own leader election service.
 *
 * <p>Once a contender has been granted leadership he has to confirm the received leader session ID
 * by calling the method {@link LeaderElection#confirmLeadership(UUID, String)}. This will notify
 * the leader election service, that the contender has accepted the leadership specified and that
 * the leader session id as well as the leader address can now be published for leader retrieval
 * services.
 */
//服务的接口，允许在一组竞争者中选出一个领导者。
//在使用此服务之前，必须调用start方法启动它。start方法将竞争者作为参数。
// 如果存在多个竞争者，则每个竞争者必须实例化其自己的领导者选举服务。
//一旦竞争者被授予领导地位，他必须通过调用LeaderElection. Confirmleadiablishing (UUID，String) 方法来确认收到的领导会话ID。
// 这将通知领导者选举服务，竞争者已接受指定的领导，并且现在可以为领导者检索服务发布领导者会话id以及领导者地址
public interface LeaderElectionService {

    /**
     * Creates a new {@link LeaderElection} instance that is registered to this {@code
     * LeaderElectionService} instance.
     *
     * @param componentId a unique identifier that refers to the stored leader information that the
     *     newly created {@link LeaderElection} manages.
     */
    //创建一个新的领导选举注册到此的实例LeaderElectionService实例。
    //参数:
    //组件id-引用新创建的存储的领导者信息的唯一标识符领导选举管理。
    LeaderElection createLeaderElection(String componentId);
}
