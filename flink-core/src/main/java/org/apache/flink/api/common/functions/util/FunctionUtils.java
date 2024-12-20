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

package org.apache.flink.api.common.functions.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RuntimeContext;

/** Utility class that contains helper methods to work with Flink {@link Function} class. */
//包含与 Flink Function类一起使用的辅助方法的实用程序类。
@Internal
public final class FunctionUtils {

    public static void openFunction(Function function, OpenContext openContext) throws Exception {
        if (function instanceof RichFunction) {
            RichFunction richFunction = (RichFunction) function;
            richFunction.open(openContext);
        }
    }

    public static void closeFunction(Function function) throws Exception {
        if (function instanceof RichFunction) {
            RichFunction richFunction = (RichFunction) function;
            richFunction.close();
        }
    }

    public static void setFunctionRuntimeContext(Function function, RuntimeContext context) {
        if (function instanceof RichFunction) {
            RichFunction richFunction = (RichFunction) function;
            richFunction.setRuntimeContext(context);
        }
    }

    public static RuntimeContext getFunctionRuntimeContext(
            Function function, RuntimeContext defaultContext) {
        if (function instanceof RichFunction) {
            RichFunction richFunction = (RichFunction) function;
            return richFunction.getRuntimeContext();
        } else {
            return defaultContext;
        }
    }

    /** Private constructor to prevent instantiation. */
    //私有构造函数以防止实例化。
    private FunctionUtils() {
        throw new RuntimeException();
    }
}
