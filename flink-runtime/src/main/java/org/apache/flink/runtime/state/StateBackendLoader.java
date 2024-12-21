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

package org.apache.flink.runtime.state;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.configuration.StateChangelogOptions;
import org.apache.flink.runtime.state.changelog.ChangelogStateBackendHandle;
import org.apache.flink.runtime.state.delegate.DelegatingStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackendFactory;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackendFactory;
import org.apache.flink.util.DynamicCodeLoadingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** This class contains utility methods to load state backends from configurations. */
//此类包含从配置加载状态后端的实用方法。
public class StateBackendLoader {

    private static final Logger LOG = LoggerFactory.getLogger(StateBackendLoader.class);

    /** Used for Loading ChangelogStateBackend. */
    //用于加载ChangelogStateBackend。
    private static final String CHANGELOG_STATE_BACKEND =
            "org.apache.flink.state.changelog.ChangelogStateBackend";

    /** Used for Loading TempChangelogStateBackend. */
    //用于加载 TempChangelogStateBackend。
    private static final String DEACTIVATED_CHANGELOG_STATE_BACKEND =
            "org.apache.flink.state.changelog.DeactivatedChangelogStateBackend";

    /** Used for loading RocksDBStateBackend. */
    //用于加载RocksDBStateBackend。
    private static final String ROCKSDB_STATE_BACKEND_FACTORY =
            "org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackendFactory";

    // ------------------------------------------------------------------------
    //  Configuration shortcut names
    // ------------------------------------------------------------------------
    /** The shortcut configuration name of the HashMap state backend. */
    //HashMap状态后端的快捷配置名称。
    public static final String HASHMAP_STATE_BACKEND_NAME = "hashmap";

    /**
     * The shortcut configuration name for the MemoryState backend that checkpoints to the
     * JobManager.
     */
    //检查点到 JobManager 的 MemoryState 后端的快捷方式配置名称
    @Deprecated public static final String MEMORY_STATE_BACKEND_NAME = "jobmanager";

    /** The shortcut configuration name for the FileSystem State backend. */
    //文件系统状态后端的快捷方式配置名称。
    @Deprecated public static final String FS_STATE_BACKEND_NAME = "filesystem";

    /** The shortcut configuration name for the RocksDB State Backend. */
    //RocksDB State Backend 的快捷方式配置名称。
    public static final String ROCKSDB_STATE_BACKEND_NAME = "rocksdb";

    // ------------------------------------------------------------------------
    //  Loading the state backend from a configuration
    // ------------------------------------------------------------------------

    /**
     * Loads the unwrapped state backend from the configuration, from the parameter 'state.backend',
     * as defined in {@link StateBackendOptions#STATE_BACKEND}.
     *
     * <p>The state backends can be specified either via their shortcut name, or via the class name
     * of a {@link StateBackendFactory}. If a StateBackendFactory class name is specified, the
     * factory is instantiated (via its zero-argument constructor) and its {@link
     * StateBackendFactory#createFromConfig(ReadableConfig, ClassLoader)} method is called.
     *
     * <p>Recognized shortcut names are '{@value StateBackendLoader#HASHMAP_STATE_BACKEND_NAME}',
     * '{@value StateBackendLoader#ROCKSDB_STATE_BACKEND_NAME}' '{@value
     * StateBackendLoader#MEMORY_STATE_BACKEND_NAME}' (Deprecated), and '{@value
     * StateBackendLoader#FS_STATE_BACKEND_NAME}' (Deprecated).
     *
     * @param config The configuration to load the state backend from
     * @param classLoader The class loader that should be used to load the state backend
     * @param logger Optionally, a logger to log actions to (may be null)
     * @return The instantiated state backend.
     * @throws DynamicCodeLoadingException Thrown if a state backend factory is configured and the
     *     factory class was not found or the factory could not be instantiated
     * @throws IllegalConfigurationException May be thrown by the StateBackendFactory when creating
     *     / configuring the state backend in the factory
     * @throws IOException May be thrown by the StateBackendFactory when instantiating the state
     *     backend
     */
    //从配置中的参数“state. backend”加载未包装的状态后端，如StateBackendOptions. STATE_BACKEND中所定义。
    //状态后端可以通过快捷方式名称或StateBackendFactory的类名来指定。
    // 如果指定了 StateBackendFactory 类名，则会实例化工厂（通过其零参数构造函数）
    // 并调用其StateBackendFactory. createFromConfig(ReadableConfig, ClassLoader)方法。
    //可识别的快捷方式名称为 ' "hashmap" '、' "rocksdb" ' '"jobmanager" '（已弃用）和 ' "filesystem" '（已弃用）。
    @Nonnull
    public static StateBackend loadStateBackendFromConfig(
            ReadableConfig config, ClassLoader classLoader, @Nullable Logger logger)
            throws IllegalConfigurationException, DynamicCodeLoadingException, IOException {

        checkNotNull(config, "config");
        checkNotNull(classLoader, "classLoader");

        final String backendName = config.get(StateBackendOptions.STATE_BACKEND);

        // by default the factory class is the backend name
        //默认情况下工厂类是后端名称
        String factoryClassName = backendName;

        switch (backendName.toLowerCase()) {
            case MEMORY_STATE_BACKEND_NAME:
                MemoryStateBackend backend =
                        new MemoryStateBackendFactory().createFromConfig(config, classLoader);

                if (logger != null) {
                    logger.warn(
                            "MemoryStateBackend has been deprecated. Please use 'hashmap' state "
                                    + "backend instead with JobManagerCheckpointStorage for equivalent "
                                    + "functionality");

                    logger.info("State backend is set to job manager {}", backend);
                }

                return backend;
            case FS_STATE_BACKEND_NAME:
                if (logger != null) {
                    logger.warn(
                            "{} state backend has been deprecated. Please use 'hashmap' state "
                                    + "backend instead.",
                            backendName.toLowerCase());
                }
                // fall through and use the HashMapStateBackend instead which
                // utilizes the same HeapKeyedStateBackend runtime implementation.
                //失败并使用 HashMapStateBackend 来代替，它利用相同的 HeapKeyedStateBackend 运行时实现。
            case HASHMAP_STATE_BACKEND_NAME:
                HashMapStateBackend hashMapStateBackend =
                        new HashMapStateBackendFactory().createFromConfig(config, classLoader);
                if (logger != null) {
                    logger.info("State backend is set to heap memory {}", hashMapStateBackend);
                }
                return hashMapStateBackend;

            case ROCKSDB_STATE_BACKEND_NAME:
                factoryClassName = ROCKSDB_STATE_BACKEND_FACTORY;

                // fall through to the 'default' case that uses reflection to load the backend
                // that way we can keep RocksDB in a separate module
                //落到使用反射加载后端的“默认”情况，这样我们就可以将 RocksDB 保留在单独的模块中

            default:
                if (logger != null) {
                    logger.info("Loading state backend via factory {}", factoryClassName);
                }

                StateBackendFactory<?> factory;
                try {
                    @SuppressWarnings("rawtypes")
                    Class<? extends StateBackendFactory> clazz =
                            Class.forName(factoryClassName, false, classLoader)
                                    .asSubclass(StateBackendFactory.class);

                    factory = clazz.newInstance();
                } catch (ClassNotFoundException e) {
                    throw new DynamicCodeLoadingException(
                            "Cannot find configured state backend factory class: " + backendName,
                            e);
                } catch (ClassCastException | InstantiationException | IllegalAccessException e) {
                    throw new DynamicCodeLoadingException(
                            "The class configured under '"
                                    + StateBackendOptions.STATE_BACKEND.key()
                                    + "' is not a valid state backend factory ("
                                    + backendName
                                    + ')',
                            e);
                }

                return factory.createFromConfig(config, classLoader);
        }
    }

    /**
     * Checks if an application-defined state backend is given, and if not, loads the state backend
     * from the configuration, from the parameter 'state.backend', as defined in {@link
     * CheckpointingOptions#STATE_BACKEND}. If no state backend is configured, this instantiates the
     * default state backend (the {@link HashMapStateBackend}).
     *
     * <p>If an application-defined state backend is found, and the state backend is a {@link
     * ConfigurableStateBackend}, this methods calls {@link
     * ConfigurableStateBackend#configure(ReadableConfig, ClassLoader)} on the state backend.
     *
     * <p>Refer to {@link #loadStateBackendFromConfig(ReadableConfig, ClassLoader, Logger)} for
     * details on how the state backend is loaded from the configuration.
     *
     * @param jobConfig The job configuration to load the state backend from
     * @param clusterConfig The cluster configuration to load the state backend from
     * @param classLoader The class loader that should be used to load the state backend
     * @param logger Optionally, a logger to log actions to (may be null)
     * @return The instantiated state backend.
     * @throws DynamicCodeLoadingException Thrown if a state backend factory is configured and the
     *     factory class was not found or the factory could not be instantiated
     * @throws IllegalConfigurationException May be thrown by the StateBackendFactory when creating
     *     / configuring the state backend in the factory
     * @throws IOException May be thrown by the StateBackendFactory when instantiating the state
     *     backend
     */
    //检查是否给出了应用程序定义的状态后端，如果没有，则从配置中的参数“state. backend”加载状态后端，
    // 如CheckpointingOptions. STATE_BACKEND中所定义。
    // 如果未配置状态后端，则会实例化默认状态后端（ HashMapStateBackend ）。
    //如果找到应用程序定义的状态后端，并且状态后端是ConfigurableStateBackend ，
    // 则此方法在状态后端上调用ConfigurableStateBackend. configure(ReadableConfig, ClassLoader) 。
    private static StateBackend loadFromApplicationOrConfigOrDefaultInternal(
            @Nullable StateBackend fromApplication,
            Configuration jobConfig,
            Configuration clusterConfig,
            ClassLoader classLoader,
            @Nullable Logger logger)
            throws IllegalConfigurationException, DynamicCodeLoadingException, IOException {

        checkNotNull(jobConfig, "jobConfig");
        checkNotNull(clusterConfig, "clusterConfig");
        checkNotNull(classLoader, "classLoader");

        // Job level config can override the cluster level config.
        // 作业级别配置可以覆盖集群级别配置。
        Configuration mergedConfig = new Configuration(clusterConfig);
        mergedConfig.addAll(jobConfig);

        final StateBackend backend;

        // In the FLINK-2.0, the state backend from application will be not supported anymore.
        // (1) the application defined state backend has precedence
        //在 FLINK-2.0 中，将不再支持应用程序的状态后端。
        // (1) 应用程序定义的状态后端优先
        if (fromApplication != null) {
            // see if this is supposed to pick up additional configuration parameters
            // 看看这是否应该获取额外的配置参数
            if (fromApplication instanceof ConfigurableStateBackend) {
                // needs to pick up configuration
                // 需要获取配置
                if (logger != null) {
                    logger.info(
                            "Using job/cluster config to configure application-defined state backend: {}",
                            fromApplication);
                }

                backend =
                        ((ConfigurableStateBackend) fromApplication)
                                // Use cluster config for backwards compatibility.
                                // 使用集群配置来实现向后兼容性。
                                .configure(clusterConfig, classLoader);
            } else {
                // keep as is!
                //保持原样！
                backend = fromApplication;
            }

            if (logger != null) {
                logger.info("Using application-defined state backend: {}", backend);
            }
        } else {
            // (2) check if the config defines a state backend
            //(2) 检查配置是否定义了状态后端
            backend = loadStateBackendFromConfig(mergedConfig, classLoader, logger);
        }

        return backend;
    }

    /**
     * This is the state backend loader that loads a {@link DelegatingStateBackend} wrapping the
     * state backend loaded from {@link
     * StateBackendLoader#loadFromApplicationOrConfigOrDefaultInternal} when delegation is enabled.
     * If delegation is not enabled, the underlying wrapped state backend is returned instead.
     *
     * @param fromApplication StateBackend defined from application
     * @param jobConfig The job level configuration to load the state backend from
     * @param clusterConfig The cluster level configuration to load the state backend from
     * @param classLoader The class loader that should be used to load the state backend
     * @param logger Optionally, a logger to log actions to (may be null)
     * @return The instantiated state backend.
     * @throws DynamicCodeLoadingException Thrown if a state backend (factory) is configured and the
     *     (factory) class was not found or could not be instantiated
     * @throws IllegalConfigurationException May be thrown by the StateBackendFactory when creating
     *     / configuring the state backend in the factory
     * @throws IOException May be thrown by the StateBackendFactory when instantiating the state
     *     backend
     */
    //这是状态后端加载器，在启用委派时加载DelegatingStateBackend该 DelegatingStateBackend
    // 包装从loadFromApplicationOrConfigOrDefaultInternal加载的状态后端。如果未启用委托，则返回底层包装状态后端。
    public static StateBackend fromApplicationOrConfigOrDefault(
            @Nullable StateBackend fromApplication,
            Configuration jobConfig,
            Configuration clusterConfig,
            ClassLoader classLoader,
            @Nullable Logger logger)
            throws IllegalConfigurationException, DynamicCodeLoadingException, IOException {

        StateBackend rootBackend =
                loadFromApplicationOrConfigOrDefaultInternal(
                        fromApplication, jobConfig, clusterConfig, classLoader, logger);

        boolean enableChangeLog =
                jobConfig
                        .getOptional(StateChangelogOptions.ENABLE_STATE_CHANGE_LOG)
                        .orElse(clusterConfig.get(StateChangelogOptions.ENABLE_STATE_CHANGE_LOG));

        StateBackend backend;
        if (enableChangeLog) {
            backend = wrapStateBackend(rootBackend, classLoader, CHANGELOG_STATE_BACKEND);
            LOG.info(
                    "State backend loader loads {} to delegate {}",
                    backend.getClass().getSimpleName(),
                    rootBackend.getClass().getSimpleName());
        } else {
            backend = rootBackend;
            LOG.info(
                    "State backend loader loads the state backend as {}",
                    backend.getClass().getSimpleName());
        }
        return backend;
    }

    /**
     * Checks whether state backend uses managed memory, without having to deserialize or load the
     * state backend.
     *
     * @param config configuration to load the state backend from.
     * @param stateBackendFromApplicationUsesManagedMemory Whether the application-defined backend
     *     uses Flink's managed memory. Empty if application has not defined a backend.
     * @param classLoader User code classloader.
     * @return Whether the state backend uses managed memory.
     */
    //检查状态后端是否使用托管内存，而无需反序列化或加载状态后端。
    public static boolean stateBackendFromApplicationOrConfigOrDefaultUseManagedMemory(
            Configuration config,
            Optional<Boolean> stateBackendFromApplicationUsesManagedMemory,
            ClassLoader classLoader) {

        checkNotNull(config, "config");

        // (1) the application defined state backend has precedence
        //(1) 应用程序定义的状态后端优先
        if (stateBackendFromApplicationUsesManagedMemory.isPresent()) {
            return stateBackendFromApplicationUsesManagedMemory.get();
        }

        // (2) check if the config defines a state backend
        //(2) 检查配置是否定义了状态后端
        try {
            final StateBackend fromConfig = loadStateBackendFromConfig(config, classLoader, LOG);
            return fromConfig.useManagedMemory();
        } catch (IllegalConfigurationException | DynamicCodeLoadingException | IOException e) {
            LOG.warn(
                    "Cannot decide whether state backend uses managed memory. Will reserve managed memory by default.",
                    e);
            return true;
        }
    }

    /**
     * Load state backend which may wrap the original state backend for recovery.
     *
     * @param originalStateBackend StateBackend loaded from application or config.
     * @param classLoader User code classloader.
     * @param keyedStateHandles The state handles for restore.
     * @return Wrapped state backend for recovery.
     * @throws DynamicCodeLoadingException Thrown if keyed state handles of wrapped state backend
     *     are found and the class was not found or could not be instantiated.
     */
    //加载状态后端，它可以包装原始状态后端以进行恢复。
    public static StateBackend loadStateBackendFromKeyedStateHandles(
            StateBackend originalStateBackend,
            ClassLoader classLoader,
            Collection<KeyedStateHandle> keyedStateHandles)
            throws DynamicCodeLoadingException {
        // Wrapping ChangelogStateBackend or ChangelogStateBackendHandle is not supported currently.
        //目前不支持包装 ChangelogStateBackend 或 ChangelogStateBackendHandle。
        if (!isChangelogStateBackend(originalStateBackend)
                && keyedStateHandles.stream()
                        .anyMatch(
                                stateHandle ->
                                        stateHandle instanceof ChangelogStateBackendHandle)) {
            return wrapStateBackend(
                    originalStateBackend, classLoader, DEACTIVATED_CHANGELOG_STATE_BACKEND);
        }
        return originalStateBackend;
    }

    public static boolean isChangelogStateBackend(StateBackend backend) {
        return CHANGELOG_STATE_BACKEND.equals(backend.getClass().getName());
    }

    private static StateBackend wrapStateBackend(
            StateBackend backend, ClassLoader classLoader, String className)
            throws DynamicCodeLoadingException {

        // ChangelogStateBackend resides in a separate module, load it using reflection
        // ChangelogStateBackend驻留在一个单独的模块中，使用反射加载它
        try {
            Constructor<? extends DelegatingStateBackend> constructor =
                    Class.forName(className, false, classLoader)
                            .asSubclass(DelegatingStateBackend.class)
                            .getDeclaredConstructor(StateBackend.class);
            constructor.setAccessible(true);
            return constructor.newInstance(backend);
        } catch (ClassNotFoundException e) {
            throw new DynamicCodeLoadingException(
                    "Cannot find DelegateStateBackend class: " + className, e);
        } catch (InstantiationException
                | IllegalAccessException
                | NoSuchMethodException
                | InvocationTargetException e) {
            throw new DynamicCodeLoadingException("Fail to initialize: " + className, e);
        }
    }

    // ------------------------------------------------------------------------

    /** This class is not meant to be instantiated. */
    //这个类不应该被实例化。
    private StateBackendLoader() {}
}
