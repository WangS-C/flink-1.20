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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.executiongraph.JobStatusListener;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This actor listens to changes in the JobStatus and activates or deactivates the periodic
 * checkpoint scheduler.
 */
public class CheckpointCoordinatorDeActivator implements JobStatusListener {

    private final CheckpointCoordinator coordinator;
    private final boolean allTasksOutputNonBlocking;

    public CheckpointCoordinatorDeActivator(
            CheckpointCoordinator coordinator, boolean allTasksOutputNonBlocking) {
        this.coordinator = checkNotNull(coordinator);
        this.allTasksOutputNonBlocking = allTasksOutputNonBlocking;
    }

    @Override
    public void jobStatusChanges(JobID jobId, JobStatus newJobStatus, long timestamp) {
        if (newJobStatus == JobStatus.RUNNING && allTasksOutputNonBlocking) {
            // start the checkpoint scheduler if there is no blocking edge
            //如果没有阻塞边缘则启动检查点调度程序
            coordinator.startCheckpointScheduler();
        } else {
            // anything else should stop the trigger for now
            //其他任何事情都应该暂时停止触发器
            coordinator.stopCheckpointScheduler();
        }
    }
}
