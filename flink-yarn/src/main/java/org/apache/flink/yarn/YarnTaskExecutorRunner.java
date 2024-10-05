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

package org.apache.flink.yarn;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RpcOptions;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.TaskManagerOptionsInternal;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.taskexecutor.TaskManagerRunner;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/** This class is the executable entry point for running a TaskExecutor in a YARN container. */
//此类是在 YARN 容器中运行 TaskExecutor 的可执行入口点。
public class YarnTaskExecutorRunner {

    protected static final Logger LOG = LoggerFactory.getLogger(YarnTaskExecutorRunner.class);

    /** The process environment variables. */
    private static final Map<String, String> ENV = System.getenv();

    /** The exit code returned if the initialization of the yarn task executor runner failed. */
    private static final int INIT_ERROR_EXIT_CODE = 31;

    // ------------------------------------------------------------------------
    //  Program entry point
    // ------------------------------------------------------------------------

    /**
     * The entry point for the YARN task executor runner.
     *
     * @param args The command line arguments.
     */
    public static void main(String[] args) {
        EnvironmentInformation.logEnvironmentInfo(LOG, "YARN TaskExecutor runner", args);
        SignalHandler.register(LOG);
        JvmShutdownSafeguard.installAsShutdownHook(LOG);

        runTaskManagerSecurely(args);
    }

    /**
     * The instance entry point for the YARN task executor. Obtains user group information and calls
     * the main work method {@link TaskManagerRunner#runTaskManager(Configuration, PluginManager)}
     * as a privileged action.
     *
     * @param args The command line arguments.
     */
    //YARN 任务执行器的实例入口点。获取用户组信息并作为特权操作调用主要工作方法TaskManagerRunner. runTaskManager(Configuration, PluginManager) 。
    //参数：
    //args – 命令行参数。
    private static void runTaskManagerSecurely(String[] args) {
        Configuration configuration = null;

        try {
            LOG.debug("All environment variables: {}", ENV);

            final String currDir = ENV.get(Environment.PWD.key());
            LOG.info("Current working Directory: {}", currDir);

            configuration = TaskManagerRunner.loadConfiguration(args);
            //设置和修改配置
            setupAndModifyConfiguration(configuration, currDir, ENV);
        } catch (Throwable t) {
            LOG.error("YARN TaskManager initialization failed.", t);
            System.exit(INIT_ERROR_EXIT_CODE);
        }

        //安全地运行TaskManager进程
        TaskManagerRunner.runTaskManagerProcessSecurely(Preconditions.checkNotNull(configuration));
    }

    @VisibleForTesting
    static void setupAndModifyConfiguration(
            Configuration configuration, String currDir, Map<String, String> variables)
            throws Exception {
        final String localDirs = variables.get(Environment.LOCAL_DIRS.key());
        LOG.info("Current working/local Directory: {}", localDirs);

        BootstrapTools.updateTmpDirectoriesInConfiguration(configuration, localDirs);

        setupConfigurationFromVariables(configuration, currDir, variables);
    }

    private static void setupConfigurationFromVariables(
            Configuration configuration, String currDir, Map<String, String> variables)
            throws IOException {
        final String yarnClientUsername = variables.get(YarnConfigKeys.ENV_HADOOP_USER_NAME);

        final String localKeytabPath = variables.get(YarnConfigKeys.LOCAL_KEYTAB_PATH);
        LOG.info("TM: local keytab path obtained {}", localKeytabPath);

        final String keytabPrincipal = variables.get(YarnConfigKeys.KEYTAB_PRINCIPAL);
        LOG.info("TM: keytab principal obtained {}", keytabPrincipal);

        // tell pekko to die in case of an error
        configuration.set(RpcOptions.JVM_EXIT_ON_FATAL_ERROR, true);

        String keytabPath = Utils.resolveKeytabPath(currDir, localKeytabPath);

        UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();

        LOG.info(
                "YARN daemon is running as: {} Yarn client user obtainer: {}",
                currentUser.getShortUserName(),
                yarnClientUsername);

        if (keytabPath != null && keytabPrincipal != null) {
            configuration.set(SecurityOptions.KERBEROS_LOGIN_KEYTAB, keytabPath);
            configuration.set(SecurityOptions.KERBEROS_LOGIN_PRINCIPAL, keytabPrincipal);
        }

        // use the hostname passed by job manager
        final String taskExecutorHostname =
                variables.get(YarnResourceManagerDriver.ENV_FLINK_NODE_ID);
        if (taskExecutorHostname != null) {
            configuration.set(TaskManagerOptions.HOST, taskExecutorHostname);
            configuration.set(
                    TaskManagerOptionsInternal.TASK_MANAGER_NODE_ID, taskExecutorHostname);
        }
    }
}
