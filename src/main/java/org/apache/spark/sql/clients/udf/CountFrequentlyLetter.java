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

import org.apache.spark.sql.api.java.UDF1;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class CountFrequentlyLetter implements UDF1<String, String> {
    public String call(String value) throws Exception {
        if (value == null || value.isEmpty()) {
            return "";
        }
        Map<Character, Integer> countMap = new HashMap<>();
        for (char c : value.toCharArray()) {
            countMap.put(c, countMap.getOrDefault(c, 0) + 1);
        }
        int maxCount = Collections.max(countMap.values());
        char maxChar = 0;
        for (Map.Entry<Character, Integer> entry : countMap.entrySet()) {
            if (entry.getValue() == maxCount) {
                maxChar = entry.getKey();
                break;
            }
        }
        String prefix = String.format("%c_%d->", maxChar, maxCount);
        return prefix + value;
    }
}
