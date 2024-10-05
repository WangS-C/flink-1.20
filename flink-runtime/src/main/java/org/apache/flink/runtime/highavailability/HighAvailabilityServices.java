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

package org.apache.flink.runtime.highavailability;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.blob.BlobStore;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.dispatcher.cleanup.GloballyCleanableResource;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.runtime.leaderelection.LeaderElection;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.concurrent.FutureUtils;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * The HighAvailabilityServices give access to all services needed for a highly-available setup. In
 * particular, the services provide access to highly available storage and registries, as well as
 * distributed counters and leader election.
 *
 * <ul>
 *   <li>ResourceManager leader election and leader retrieval
 *   <li>JobManager leader election and leader retrieval
 *   <li>Persistence for checkpoint metadata
 *   <li>Registering the latest completed checkpoint(s)
 *   <li>Persistence for the BLOB store
 *   <li>Registry that marks a job's status
 *   <li>Naming of RPC endpoints
 * </ul>
 */
//HighAvailabilityServices 允许访问高可用性设置所需的所有服务。
// 特别是，这些服务提供对高可用存储和注册表的访问，以及分布式计数器和领导者选举。
//ResourceManager 领导者选举和领导者检索
//JobManager 领导者选举和领导者检索
//检查点元数据的持久性
//注册最新完成的检查点
//BLOB 存储的持久性
//标记作业状态的注册表
//RPC端点的命名
public interface HighAvailabilityServices
        extends ClientHighAvailabilityServices, GloballyCleanableResource {

    // ------------------------------------------------------------------------
    //  Constants
    // ------------------------------------------------------------------------

    /**
     * This UUID should be used when no proper leader election happens, but a simple pre-configured
     * leader is used. That is for example the case in non-highly-available standalone setups.
     */
//    当没有发生正确的领导者选举但使用简单的预配置领导者时，应该使用此 UUID。例如，在非高可用性独立设置中就是这种情况。
    UUID DEFAULT_LEADER_ID = new UUID(0, 0);

    /**
     * This JobID should be used to identify the old JobManager when using the {@link
     * HighAvailabilityServices}. With the new mode every JobMaster will have a distinct JobID
     * assigned.
     */
//    使用HighAvailabilityServices时，应使用此 JobID 来识别旧的 JobManager。
//    在新模式下，每个 JobMaster 都会分配一个不同的 JobID
    JobID DEFAULT_JOB_ID = new JobID(0L, 0L);

    // ------------------------------------------------------------------------
    //  Services
    // ------------------------------------------------------------------------

    /** Gets the leader retriever for the cluster's resource manager. */
//    获取集群资源管理器的领导者检索器
    LeaderRetrievalService getResourceManagerLeaderRetriever();

    /**
     * Gets the leader retriever for the dispatcher. This leader retrieval service is not always
     * accessible.
     */
//    获取调度程序的领导者检索器。此领导者检索服务并不总是可以访问。
    LeaderRetrievalService getDispatcherLeaderRetriever();

    /**
     * Gets the leader retriever for the job JobMaster which is responsible for the given job.
     *
     * @param jobID The identifier of the job.
     *
     * @return Leader retrieval service to retrieve the job manager for the given job
     *
     * @deprecated This method should only be used by the legacy code where the JobManager acts as
     *         the master.
     */
    @Deprecated
    LeaderRetrievalService getJobManagerLeaderRetriever(JobID jobID);

    /**
     * Gets the leader retriever for the job JobMaster which is responsible for the given job.
     *
     * @param jobID The identifier of the job.
     * @param defaultJobManagerAddress JobManager address which will be returned by a static leader
     *         retrieval service.
     *
     * @return Leader retrieval service to retrieve the job manager for the given job
     */
//    获取负责给定作业的作业 JobMaster 的领导者检索器。
//参数：
//jobID – 作业的标识符。
// defaultJobManagerAddress – 将由静态领导者检索服务返回的 JobManager 地址。
//返回：
//领导者检索服务，用于检索给定作业的作业管理器
    LeaderRetrievalService getJobManagerLeaderRetriever(
            JobID jobID, String defaultJobManagerAddress);

    /**
     * This retriever should no longer be used on the cluster side. The web monitor retriever is
     * only required on the client-side and we have a dedicated high-availability services for the
     * client, named {@link ClientHighAvailabilityServices}. See also FLINK-13750.
     *
     * @return the leader retriever for web monitor
     *
     * @deprecated just use {@link #getClusterRestEndpointLeaderRetriever()} instead of this method.
     */
    @Deprecated
    default LeaderRetrievalService getWebMonitorLeaderRetriever() {
        throw new UnsupportedOperationException(
                "getWebMonitorLeaderRetriever should no longer be used. Instead use "
                        + "#getClusterRestEndpointLeaderRetriever to instantiate the cluster "
                        + "rest endpoint leader retriever. If you called this method, then "
                        + "make sure that #getClusterRestEndpointLeaderRetriever has been "
                        + "implemented by your HighAvailabilityServices implementation.");
    }

    /** Gets the {@link LeaderElection} for the cluster's resource manager. */
//    获取集群资源管理器的LeaderElection 。
    LeaderElection getResourceManagerLeaderElection();

    /** Gets the {@link LeaderElection} for the cluster's dispatcher. */
//    获取集群调度程序的LeaderElection 。
    LeaderElection getDispatcherLeaderElection();

    /** Gets the {@link LeaderElection} for the job with the given {@link JobID}. */
//    获取具有给定JobID的作业的LeaderElection 。
    LeaderElection getJobManagerLeaderElection(JobID jobID);

    /**
     * Gets the {@link LeaderElection} for the cluster's rest endpoint.
     *
     * @deprecated Use {@link #getClusterRestEndpointLeaderElection()} instead.
     */
    @Deprecated
    default LeaderElection getWebMonitorLeaderElection() {
        throw new UnsupportedOperationException(
                "getWebMonitorLeaderElectionService should no longer be used. Instead use "
                        + "#getClusterRestEndpointLeaderElectionService to instantiate the cluster "
                        + "rest endpoint's leader election service. If you called this method, then "
                        + "make sure that #getClusterRestEndpointLeaderElectionService has been "
                        + "implemented by your HighAvailabilityServices implementation.");
    }

    /**
     * Gets the checkpoint recovery factory for the job manager.
     *
     * @return Checkpoint recovery factory
     */
//    获取作业管理器的检查点恢复工厂。
//返回：
//检查点恢复工厂
    CheckpointRecoveryFactory getCheckpointRecoveryFactory() throws Exception;

    /**
     * Gets the submitted job graph store for the job manager.
     *
     * @return Submitted job graph store
     *
     * @throws Exception if the submitted job graph store could not be created
     */
//    获取作业管理器提交的作业图存储。
//返回：
//提交的作业图存储
    JobGraphStore getJobGraphStore() throws Exception;

    /**
     * Gets the store that holds information about the state of finished jobs.
     *
     * @return Store of finished job results
     *
     * @throws Exception if job result store could not be created
     */
//    获取保存有关已完成作业状态信息的存储。
//返回：
//存储已完成的作业结果
    JobResultStore getJobResultStore() throws Exception;

    /**
     * Creates the BLOB store in which BLOBs are stored in a highly-available fashion.
     *
     * @return Blob store
     *
     * @throws IOException if the blob store could not be created
     */
//    创建 BLOB 存储，其中以高可用性方式存储 BLOB。
//返回：
//斑点商店
    BlobStore createBlobStore() throws IOException;

    /** Gets the {@link LeaderElection} for the cluster's rest endpoint. */
    default LeaderElection getClusterRestEndpointLeaderElection() {
        // for backwards compatibility we delegate to getWebMonitorLeaderElectionService
        // all implementations of this interface should override
        // getClusterRestEndpointLeaderElectionService, though
        return getWebMonitorLeaderElection();
    }

    @Override
    default LeaderRetrievalService getClusterRestEndpointLeaderRetriever() {
        // for backwards compatibility we delegate to getWebMonitorLeaderRetriever
        // all implementations of this interface should override
        // getClusterRestEndpointLeaderRetriever, though
        return getWebMonitorLeaderRetriever();
    }

    // ------------------------------------------------------------------------
    //  Shutdown and Cleanup
    // ------------------------------------------------------------------------

    /**
     * Closes the high availability services, releasing all resources.
     *
     * <p>This method <b>does not delete or clean up</b> any data stored in external stores (file
     * systems, ZooKeeper, etc). Another instance of the high availability services will be able to
     * recover the job.
     *
     * <p>If an exception occurs during closing services, this method will attempt to continue
     * closing other services and report exceptions only after all services have been attempted to
     * be closed.
     *
     * @throws Exception Thrown, if an exception occurred while closing these services.
     */
    @Override
    void close() throws Exception;

    /**
     * Deletes all data stored by high availability services in external stores.
     *
     * <p>After this method was called, any job or session that was managed by these high
     * availability services will be unrecoverable.
     *
     * <p>If an exception occurs during cleanup, this method will attempt to continue the cleanup
     * and report exceptions only after all cleanup steps have been attempted.
     *
     * @throws Exception if an error occurred while cleaning up data stored by them.
     */
//    删除外部存储中高可用性服务存储的所有数据。
//调用此方法后，由这些高可用性服务管理的任何作业或会话都将不可恢复。
//如果清理期间发生异常，此方法将尝试继续清理并仅在尝试所有清理步骤后报告异常。
    void cleanupAllData() throws Exception;

    /**
     * Calls {@link #cleanupAllData()} (if {@code true} is passed as a parameter) before calling
     * {@link #close()} on this instance. Any error that appeared during the cleanup will be
     * propagated after calling {@code close()}.
     */
    default void closeWithOptionalClean(boolean cleanupData) throws Exception {
        Throwable exception = null;
        if (cleanupData) {
            try {
                cleanupAllData();
            } catch (Throwable t) {
                exception = ExceptionUtils.firstOrSuppressed(t, exception);
            }
        }
        try {
            close();
        } catch (Throwable t) {
            exception = ExceptionUtils.firstOrSuppressed(t, exception);
        }

        if (exception != null) {
            ExceptionUtils.rethrowException(exception);
        }
    }

    @Override
    default CompletableFuture<Void> globalCleanupAsync(JobID jobId, Executor executor) {
        return FutureUtils.completedVoidFuture();
    }
}
