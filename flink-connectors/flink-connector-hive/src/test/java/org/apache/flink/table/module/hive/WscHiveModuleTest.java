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

package org.apache.flink.table.module.hive;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;

import org.junit.Test;

/** Test for {@link HiveModule}. */
public class WscHiveModuleTest {

    @Test
    public  void testSql(){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(new Configuration());

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();

        TableEnvironmentImpl tableEnv = (TableEnvironmentImpl) StreamTableEnvironment.create(env, settings);
        tableEnv.executeSql("create table source_table(uid VARCHAR,\n"
                + "username VARCHAR,\n"
                + "proc_time as procTime()) with ('connector' = 'datagen',\n"
                + "'fields.uid.kind' = 'sequence',\n"
                + "'fields.uid.start' = '12',\n"
                + "'fields.uid.end' = '1000',\n"
                + "'rows-per-second' = '1');");

        tableEnv.executeSql("create table sink_table(uid VARCHAR,\n"
                + "username VARCHAR) with ( 'connector' = 'print');");

        StatementSet statementSet = tableEnv.createStatementSet();

        statementSet.addInsertSql("insert\n"
                + "\tinto\n"
                + "\tsink_table\n"
                + "select\n"
                + "\tuid ,\n"
                + "\tcast(REGEXP_EXTRACT(username, '(.*?)([0-9]+)(.*?)', 2) as VARCHAR)\n"
                + "from\n"
                + "\tsource_table;");

        statementSet.execute();
    }

}
