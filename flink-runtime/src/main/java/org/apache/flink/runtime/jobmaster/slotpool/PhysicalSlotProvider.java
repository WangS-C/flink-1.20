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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.jobmaster.SlotRequestId;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** The provider serves physical slot requests. */
public interface PhysicalSlotProvider {

    /**
     * Submit requests to allocate physical slots.
     *
     * <p>The physical slot can be either allocated from the slots, which are already available for
     * the job, or a new one can be requested from the resource manager.
     *
     * @param physicalSlotRequests physicalSlotRequest slot requirements
     * @return futures of the allocated slots
     */
    //提交分配物理槽位的请求。
    //物理槽可以从已经可用于作业的槽中分配，也可以从资源管理器请求一个新的槽。
    //参数：
    //physicalSlotRequests –physicalSlotRequest 槽位要求
    //返回：
    //分配时段的期货
    Map<SlotRequestId, CompletableFuture<PhysicalSlotRequest.Result>> allocatePhysicalSlots(
            Collection<PhysicalSlotRequest> physicalSlotRequests);

    /**
     * Cancels the slot request with the given {@link SlotRequestId}.
     *
     * <p>If the request is already fulfilled with a physical slot, the slot will be released.
     *
     * @param slotRequestId identifying the slot request to cancel
     * @param cause of the cancellation
     */
    void cancelSlotRequest(SlotRequestId slotRequestId, Throwable cause);

    /**
     * Disables batch slot request timeout check. Invoked when someone else wants to take over the
     * timeout check responsibility.
     */
    void disableBatchSlotRequestTimeoutCheck();
}
