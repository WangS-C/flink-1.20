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

package org.apache.flink.runtime.leaderretrieval;

/**
 * This interface has to be implemented by a service which retrieves the current leader and notifies
 * a listener about it.
 *
 * <p>Prior to using this service it has to be started by calling the start method. The start method
 * also takes the {@link LeaderRetrievalListener} as an argument. The service can only be started
 * once.
 *
 * <p>The service should be stopped by calling the stop method.
 */
//此接口必须由检索当前leader并通知侦听器的服务来实现。
//在使用此服务之前，必须通过调用start方法来启动它。start方法还将LeaderRetrievalListener作为参数。该服务只能启动一次。
//应通过调用stop方法停止服务。
public interface LeaderRetrievalService {

    /**
     * Starts the leader retrieval service with the given listener to listen for new leaders. This
     * method can only be called once.
     *
     * @param listener The leader retrieval listener which will be notified about new leaders.
     * @throws Exception
     */
    //使用给定的侦听器启动leader检索服务，以侦听新的leader。此方法只能调用一次。
    //参数:
    //监听器-领导者检索侦听器，将收到有关新领导者的通知。
    void start(LeaderRetrievalListener listener) throws Exception;

    /**
     * Stops the leader retrieval service.
     *
     * @throws Exception
     */
    //停止leader检索服务。
    void stop() throws Exception;
}
