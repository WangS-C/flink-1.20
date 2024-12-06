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

package org.apache.flink.table.api;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.PublicEvolving;

/**
 * A {@link StatementSet} accepts pipelines defined by DML statements or {@link Table} objects. The
 * planner can optimize all added statements together and then submit them as one job.
 *
 * <p>The added statements will be cleared when calling the {@link #execute()} method.
 */
@PublicEvolving
public interface StatementSet extends Explainable<StatementSet>, Compilable, Executable {

    /** Adds a {@link TablePipeline}. */
    StatementSet add(TablePipeline tablePipeline);

    /** Adds an {@code INSERT INTO} SQL statement. */
    StatementSet addInsertSql(String statement);

    /**
     * Shorthand for {@code statementSet.add(table.insertInto(targetPath))}.
     *
     * @see #add(TablePipeline)
     * @see Table#insertInto(String)
     */
    StatementSet addInsert(String targetPath, Table table);

    /**
     * Shorthand for {@code statementSet.add(table.insertInto(targetPath, overwrite))}.
     *
     * @see #add(TablePipeline)
     * @see Table#insertInto(String, boolean)
     */
    StatementSet addInsert(String targetPath, Table table, boolean overwrite);

    /**
     * Shorthand for {@code statementSet.add(table.insertInto(targetDescriptor))}.
     *
     * @see #add(TablePipeline)
     * @see Table#insertInto(TableDescriptor)
     */
    StatementSet addInsert(TableDescriptor targetDescriptor, Table table);

    /**
     * Shorthand for {@code statementSet.add(table.insertInto(targetDescriptor, overwrite))}.
     *
     * @see #add(TablePipeline)
     * @see Table#insertInto(TableDescriptor, boolean)
     */
    StatementSet addInsert(TableDescriptor targetDescriptor, Table table, boolean overwrite);

    /**
     * {@inheritDoc}
     *
     * <p>This method executes all statements as one job.
     *
     * <p>The added statements will be cleared after calling this method.
     */
    //执行此对象。
    //默认情况下，所有DML操作都是异步执行的。
    // 使用TableResult. await() 或TableResult. getJobClient() 监视执行。
    // 将TableConfigOptions. TABLE_DML_SYNC设置为始终同步执行。
    //此方法将所有语句作为一个作业执行。
    //添加的语句将在调用此方法后清除。
    @Override
    TableResult execute();

    /**
     * {@inheritDoc}
     *
     * <p>This method compiles all statements into a {@link CompiledPlan} that can be executed as
     * one job.
     */
    @Override
    @Experimental
    CompiledPlan compilePlan() throws TableException;
}
