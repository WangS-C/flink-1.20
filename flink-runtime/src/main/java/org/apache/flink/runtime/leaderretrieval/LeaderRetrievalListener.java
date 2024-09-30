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

import javax.annotation.Nullable;

import java.util.UUID;

/**
 * Classes which want to be notified about a changing leader by the {@link LeaderRetrievalService}
 * have to implement this interface.
 */
//希望由LeaderRetrievalService通知有关更改领导者的类必须实现此接口。
public interface LeaderRetrievalListener {

    /**
     * This method is called by the {@link LeaderRetrievalService} when a new leader is elected.
     *
     * <p>If both arguments are null then it signals that leadership was revoked without a new
     * leader having been elected.
     *
     * @param leaderAddress The address of the new leader
     * @param leaderSessionID The new leader session ID
     */
    //当选新的领导者时， LeaderRetrievalService将调用此方法。
    //如果两个参数都为空，则表示在没有选举新领导人的情况下领导层被撤销。
    //参数:
    //leaderAddress -新领导者 的地址
    //leaderSessionID -新领导者会话ID
    void notifyLeaderAddress(@Nullable String leaderAddress, @Nullable UUID leaderSessionID);

    /**
     * This method is called by the {@link LeaderRetrievalService} in case of an exception. This
     * assures that the {@link LeaderRetrievalListener} is aware of any problems occurring in the
     * {@link LeaderRetrievalService} thread.
     *
     * @param exception
     */
    //此方法由LeaderRetrievalService在发生异常时调用。这确保LeaderRetrievalListener知道在LeaderRetrievalService线程中发生的任何问题
    void handleError(Exception exception);
}
