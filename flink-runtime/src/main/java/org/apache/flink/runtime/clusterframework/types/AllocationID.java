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

package org.apache.flink.runtime.clusterframework.types;

import org.apache.flink.util.AbstractID;
import org.apache.flink.util.StringUtils;

/**
 * Unique identifier for a physical slot allocated by a JobManager via the ResourceManager from a
 * TaskManager. The ID is assigned once the JobManager (or its SlotPool) first requests the slot and
 * is constant across retries.
 *
 * <p>This ID is used by the TaskManager and ResourceManager to track and synchronize which slots
 * are allocated to which JobManager and which are free.
 *
 * <p>In contrast to this AllocationID, the {@link org.apache.flink.runtime.jobmaster.SlotRequestId}
 * is used when a task requests a logical slot from the SlotPool. Multiple logical slot requests can
 * map to one physical slot request (due to slot sharing).
 */
//由JobManager通过ResourceManager从TaskManager分配的物理插槽的唯一标识符。
// 一旦JobManager (或其SlotPool) 首次请求插槽，就会分配ID，并且在重试期间保持不变。
//TaskManager和ResourceManager使用此ID来跟踪和同步哪些slot分配给哪些JobManager以及哪些是空闲的。
//与此AllocationID相反，当任务从SlotPool请求逻辑槽时，使用org. Apache. flink. runtime. jobmaster. SlotRequestId。
// 多个逻辑时隙请求可以映射到一个物理时隙请求 (由于时隙共享)。
public class AllocationID extends AbstractID {

    private static final long serialVersionUID = 1L;

    /** Constructs a new random AllocationID. */
    public AllocationID() {
        super();
    }

    private AllocationID(byte[] bytes) {
        super(bytes);
    }

    /**
     * Constructs a new AllocationID with the given parts.
     *
     * @param lowerPart the lower bytes of the ID
     * @param upperPart the higher bytes of the ID
     */
    public AllocationID(long lowerPart, long upperPart) {
        super(lowerPart, upperPart);
    }

    public static AllocationID fromHexString(String hexString) {
        return new AllocationID(StringUtils.hexStringToByte(hexString));
    }
}
