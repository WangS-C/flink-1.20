/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.operators.coordination.OperatorEventDispatcher;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeServiceAware;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import java.util.Optional;
import java.util.function.Supplier;

/** A utility to instantiate new operators with a given factory. */
public class StreamOperatorFactoryUtil {
    /**
     * Creates a new operator using a factory and makes sure that all special factory traits are
     * properly handled.
     *
     * @param operatorFactory the operator factory.
     * @param containingTask the containing task.
     * @param configuration the configuration of the operator.
     * @param output the output of the operator.
     * @param operatorEventDispatcher the operator event dispatcher for communication between
     *     operator and coordinators.
     * @return a newly created and configured operator, and the {@link ProcessingTimeService}
     *     instance it can access.
     */
    //使用工厂创建一个新的运算符，并确保所有特殊的工厂特征都得到正确处理。
    public static <OUT, OP extends StreamOperator<OUT>>
            Tuple2<OP, Optional<ProcessingTimeService>> createOperator(
                    StreamOperatorFactory<OUT> operatorFactory,
                    StreamTask<OUT, ?> containingTask,
                    StreamConfig configuration,
                    Output<StreamRecord<OUT>> output,
                    OperatorEventDispatcher operatorEventDispatcher) {

        MailboxExecutor mailboxExecutor =
                containingTask
                        .getMailboxExecutorFactory()
                        .createExecutor(configuration.getChainIndex());

        if (operatorFactory instanceof YieldingOperatorFactory) {
            ((YieldingOperatorFactory<?>) operatorFactory).setMailboxExecutor(mailboxExecutor);
        }

        final Supplier<ProcessingTimeService> processingTimeServiceFactory =
                () ->
                        containingTask
                                .getProcessingTimeServiceFactory()
                                .createProcessingTimeService(mailboxExecutor);

        final ProcessingTimeService processingTimeService;
        if (operatorFactory instanceof ProcessingTimeServiceAware) {
            processingTimeService = processingTimeServiceFactory.get();
            ((ProcessingTimeServiceAware) operatorFactory)
                    .setProcessingTimeService(processingTimeService);
        } else {
            processingTimeService = null;
        }

        // TODO: what to do with ProcessingTimeServiceAware?
        OP op =
                operatorFactory.createStreamOperator(
                        new StreamOperatorParameters<>(
                                containingTask,
                                configuration,
                                output,
                                processingTimeService != null
                                        ? () -> processingTimeService
                                        : processingTimeServiceFactory,
                                operatorEventDispatcher,
                                mailboxExecutor));
        if (op instanceof YieldingOperator) {
            ((YieldingOperator<?>) op).setMailboxExecutor(mailboxExecutor);
        }
        return new Tuple2<>(op, Optional.ofNullable(processingTimeService));
    }
}
