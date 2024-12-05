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

package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsStaging;

import javax.annotation.Nullable;

import java.util.Map;

/**
 * DML operation that tells to write to a sink which implements {@link SupportsStaging}. Currently.
 * this operation is for CTAS(CREATE TABLE AS SELECT) and RTAS([CREATE OR] REPLACE TABLE AS SELECT)
 * statement.
 *
 * <p>StagedSinkModifyOperation is an extension of SinkModifyOperation in the atomic CTAS/RTAS
 * scenario. Whiling checking whether the corresponding sink support atomic CTAS/RTAS or not, we
 * will need to get DynamicTableSink firstly and check whether it implements {@link SupportsStaging}
 * and then call the method {@link SupportsStaging#applyStaging}. We maintain the DynamicTableSink
 * in this operation so that we can reuse this DynamicTableSink instead of creating a new
 * DynamicTableSink during translating the operation again which is error-prone.
 */
//DML操作，告诉写入实现SupportsStaging的接收器。
// 当前。此操作针对CTAS(CREATE TABLE AS SELECT) 和RTAS([CREATE OR] REPLACE TABLE AS SELECT) 语句。
//StagedSinkModifyOperation是原子CTAS/ RTAS方案中SinkModifyOperation的扩展。
// 在检查相应的接收器是否支持原子CTAS/ RTAS时，我们需要首先获取DynamicTableSink并检查它是否实现了SupportsStaging ，
// 然后调用SupportsStaging. applyStaging方法。
// 我们在此操作中维护DynamicTableSink，以便我们可以重用此DynamicTableSink，
// 而不是在再次翻译操作期间创建新的DynamicTableSink，这很容易出错。
@Internal
public class StagedSinkModifyOperation extends SinkModifyOperation {

    private final DynamicTableSink dynamicTableSink;

    public StagedSinkModifyOperation(
            ContextResolvedTable contextResolvedTable,
            QueryOperation child,
            Map<String, String> staticPartitions,
            int[][] targetColumns,
            boolean overwrite,
            Map<String, String> dynamicOptions,
            DynamicTableSink dynamicTableSink) {
        this(
                contextResolvedTable,
                child,
                staticPartitions,
                targetColumns,
                overwrite,
                dynamicOptions,
                ModifyType.INSERT,
                dynamicTableSink);
    }

    public StagedSinkModifyOperation(
            ContextResolvedTable contextResolvedTable,
            QueryOperation child,
            Map<String, String> staticPartitions,
            @Nullable int[][] targetColumns,
            boolean overwrite,
            Map<String, String> dynamicOptions,
            ModifyType modifyType,
            DynamicTableSink dynamicTableSink) {
        super(
                contextResolvedTable,
                child,
                staticPartitions,
                targetColumns,
                overwrite,
                dynamicOptions,
                modifyType);
        this.dynamicTableSink = dynamicTableSink;
    }

    public DynamicTableSink getDynamicTableSink() {
        return dynamicTableSink;
    }
}
