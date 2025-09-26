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

package org.apache.spark.sql.clients.udf;

import org.apache.spark.sql.api.java.UDF3;

public class ReplaceAndCountUDF implements UDF3<String, String, String, String> {
    @Override
    public String call(String value, String target, String dest) throws Exception {
        if (value == null || target == null || target.length() != 1 || dest == null || dest.length() != 1) {
            return null;
        }
        char ch = target.charAt(0);
        int cnt = 0;
        StringBuilder sb = new StringBuilder();
        for (char c : value.toCharArray()) {
            if (c == ch) {
                sb.append('+').append(ch).append(dest);
                cnt++;
            } else {
                sb.append(c);
            }
        }
        return target + "_" + cnt + "->" + sb.toString();
    }
}