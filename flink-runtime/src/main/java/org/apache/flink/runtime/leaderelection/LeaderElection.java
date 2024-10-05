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
 * {@code LeaderElection} serves as a proxy between {@code LeaderElectionService} and {@link
 * LeaderContender}.
 */
//LeaderElectionService和LeaderContender之间充当LeaderElectionService的代理
public interface LeaderElection extends AutoCloseable {

    /** Registers the passed {@link LeaderContender} with the leader election process. */
    //将通过的LeaderContender注册到领导者选举流程。
    void startLeaderElection(LeaderContender contender) throws Exception;

    /**
     * Confirms that the {@link LeaderContender} has accepted the leadership identified by the given
     * leader session id. It also publishes the leader address under which the leader is reachable.
     *
     * <p>The intention of this method is to establish an order between setting the new leader
     * session ID in the {@link LeaderContender} and publishing the new leader session ID and the
     * related leader address to the leader retrieval services.
     *
     * @param leaderSessionID The new leader session ID
     * @param leaderAddress The address of the new leader
     */
    //确认LeaderContender已接受由给定领导会话id标识的领导。它还会发布领导者地址，在该地址下可以访问领导者。
    //此方法的目的是在LeaderContender中设置新的leader会话ID与
    //将新的leader会话ID和相关的leader地址发布到leader检索服务之间建立顺序。
    //参数:
    //leaderAddress -新的领导者会话ID
    //leaderAddress -新的领导者
    void confirmLeadership(UUID leaderSessionID, String leaderAddress);

    /**
     * Returns {@code true} if the service's {@link LeaderContender} has the leadership under the
     * given leader session ID acquired.
     *
     * @param leaderSessionId identifying the current leader
     * @return true if the associated {@link LeaderContender} is the leader, otherwise false
     */
    //如果服务的LeaderContender在获取的给定领导者会话ID下具有领导，则返回true。
    //参数:
    //leaderSessionId -识别当前领导者
    //return:
    //如果关联的LeaderContender是领导者，则为true，否则为false
    boolean hasLeadership(UUID leaderSessionId);

    /**
     * Closes the {@code LeaderElection} by deregistering the {@link LeaderContender} from the
     * underlying leader election. {@link LeaderContender#revokeLeadership()} will be called if the
     * service still holds the leadership.
     */
    //通过从基础leader选举中取消注册LeaderContender来关闭LeaderElection。
    //LeaderContender. revokeLeadership() 如果服务仍然拥有领导地位，将被调用。
    void close() throws Exception;
}
