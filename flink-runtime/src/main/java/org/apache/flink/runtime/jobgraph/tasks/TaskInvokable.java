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
package org.apache.flink.runtime.jobgraph.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;

import javax.annotation.Nullable;

/**
 * An invokable part of the task.
 *
 * <p>The TaskManager first calls the {@link #restore} method when executing a task. If the call
 * succeeds and the task isn't cancelled then TM proceeds to {@link #invoke()}. All operations of
 * the task happen in these two methods (setting up input output stream readers and writers as well
 * as the task's core operation).
 *
 * <p>After that, {@link #cleanUp(Throwable)} is called (regardless of an failures or cancellations
 * during the above calls).
 *
 * <p>Implementations must have a constructor with a single argument of type {@link
 * org.apache.flink.runtime.execution.Environment}.
 *
 * <p><i>Developer note: While constructors cannot be enforced at compile time, we did not yet
 * venture on the endeavor of introducing factories (it is only an internal API after all, and with
 * Java 8, one can use {@code Class::new} almost like a factory lambda.</i>
 *
 * @see CheckpointableTask
 * @see CoordinatedTask
 * @see AbstractInvokable
 */
//任务的不可调用部分。
//TaskManager 在执行任务时首先调用该 restore 方法。如果调用成功且任务未取消，则 TM 将继续执行 invoke()。
// 任务的所有操作都通过这两种方法进行（设置输入、输出、流读取器和写入器，以及任务的核心操作）。
//之后， cleanUp(Throwable) 被调用（无论在上述调用期间是否失败或取消）。
//实现必须具有具有类型 org.apache.flink.runtime.execution.Environment为单个参数的构造函数。
//开发者说明：虽然构造函数不能在编译时强制执行，但我们还没有尝试引入工厂（毕竟它只是一个内部API，而在Java 8中，
// 人们几乎可以像工厂lambda一样使用 Class::new 。
@Internal
public interface TaskInvokable {

    /**
     * Starts the execution.
     *
     * <p>This method is called by the task manager when the actual execution of the task starts.
     *
     * <p>All resources should be cleaned up by calling {@link #cleanUp(Throwable)} after the method
     * returns.
     */
    //开始执行。
    //当任务实际执行开始时，任务管理器会调用此方法。
    //方法返回后，应通过调用cleanUp(Throwable)来清理所有资源。
    void invoke() throws Exception;

    /**
     * This method can be called before {@link #invoke()} to restore an invokable object for the
     * last valid state, if it has it.
     *
     * <p>If {@link #invoke()} is not called after this method for some reason (e.g. task
     * cancellation); then all resources should be cleaned up by calling {@link #cleanUp(Throwable)}
     * ()} after the method returns.
     */
    //可以在invoke() 之前调用此方法，以恢复上一个有效状态的可调用对象 (如果有)。
    //如果由于某种原因 (例如任务取消) 在此方法之后未调用invoke(); 则应在方法返回后通过调用cleanUp(Throwable)} 来清理所有资源。
    void restore() throws Exception;

    /**
     * Cleanup any resources used in {@link #invoke()} OR {@link #restore()}. This method must be
     * called regardless whether the aforementioned calls succeeded or failed.
     *
     * @param throwable iff failure happened during the execution of {@link #restore()} or {@link
     *     #invoke()}, null otherwise.
     *     <p>ATTENTION: {@link org.apache.flink.runtime.execution.CancelTaskException
     *     CancelTaskException} should not be treated as a failure.
     */
    //清理invoke()或restore()中使用的所有资源。无论上述调用成功还是失败，都必须调用该方法。
    void cleanUp(@Nullable Throwable throwable) throws Exception;

    /**
     * This method is called when a task is canceled either as a result of a user abort or an
     * execution failure. It can be overwritten to respond to shut down the user code properly.
     */
    //当任务因用户中止或执行失败而取消时，将调用此方法。它可以被覆盖以响应正确关闭用户代码
    void cancel() throws Exception;

    /**
     * @return true if blocking input such as {@link InputGate#getNext()} is used (as opposed to
     *     {@link InputGate#pollNext()}. To be removed together with the DataSet API.
     */
    //返回：
    //如果使用阻塞输入（例如InputGate. getNext() （而不是InputGate. pollNext() ，则为 true 。
    // 与 DataSet API 一起删除。
    boolean isUsingNonBlockingInput();

    /**
     * Checks whether the task should be interrupted during cancellation and if so, execute the
     * specified {@code Runnable interruptAction}.
     *
     * @param toInterrupt
     * @param taskName optional taskName to log stack trace
     * @param timeout optional timeout to log stack trace
     */
    //检查任务在取消期间是否应被中断，如果是，则执行指定的Runnable interruptAction 。
    void maybeInterruptOnCancel(
            Thread toInterrupt, @Nullable String taskName, @Nullable Long timeout);
}
