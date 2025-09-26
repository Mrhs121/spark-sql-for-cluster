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

package org.apache.spark.sql.clients;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.clients.udf.CountFrequentlyLetter;
import org.apache.spark.sql.clients.udf.ReplaceAndCountUDF;
import org.apache.spark.sql.execution.ExplainMode;
import org.apache.spark.sql.execution.ExtendedMode;
import org.apache.spark.sql.execution.HiveResult;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.System.exit;

public class SparkSQLCli {
    private static final Logger logger = LoggerFactory.getLogger(SparkSQLCli.class);

    public static void main(String[] args) {
        if (args.length < 1) {
            logger.error("Usage: org.apache.spark.examples.SparkSQLCli <input params json>");
            exit(1);
        }
        logger.info(args[0]);
        ObjectMapper mapper = new ObjectMapper();
        InputParams inputParams = null;
        try {
            inputParams = mapper.readValue(args[0], InputParams.class);
        } catch (Exception e) {
            logger.error("Failed to parse input params[{}]: {}", args[0], e.getMessage());
            exit(1);
        }

        SparkSession.Builder builder = SparkSession.builder()
                .appName("Spark SQL Cli");
        int dataSourceCount = inputParams.getDatasources().size();
        boolean isSingleDataSource = dataSourceCount == 1;
        for (DataSource ds : inputParams.getDatasources()) {
            String catalogName = ds.getCatalogName();
            if (isSingleDataSource) {
                builder.config("spark.sql.defaultCatalog", catalogName);
                logger.info("Use single catalog: {}", catalogName);
            }
            builder.config("spark.sql.catalog." + catalogName,
                    "org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog");
            builder.config("spark.sql.catalog." + catalogName + ".url", ds.getUrl());
            builder.config("spark.sql.catalog." + catalogName + ".user", ds.getUser());
            builder.config("spark.sql.catalog." + catalogName + ".password", ds.getPassword());
            logger.info("Register catalog: {}", catalogName);
        }
        builder.config("spark.kubernetes.driver.service.deleteOnTermination", "false");

        builder.enableHiveSupport();

        System.out.println("Start to init spark session");
        SparkSession spark = builder.getOrCreate();

        if (inputParams.getUdfs() != null && !inputParams.getUdfs().isEmpty()) {
            for (UDFInfo udf : inputParams.getUdfs()) {
                logger.info("Register UDF: {} -> {}", udf.getFuncName(), udf.getClassName());
                spark.udf().registerJava(udf.getFuncName(), udf.getClassName(), DataTypes.StringType);
            }
        }
        // for udf test
        spark.udf().registerJava("replace_and_count", ReplaceAndCountUDF.class.getName(), DataTypes.StringType);
        spark.udf().registerJava("count_frequently_letter", CountFrequentlyLetter.class.getName(), DataTypes.StringType);

        long startTime = System.currentTimeMillis();
        for (String singleSql : inputParams.getSql().split(";")) {
            if (StringUtils.isBlank(singleSql)) {
                continue;
            }
            System.out.println("run sql");
            beautySql(singleSql);
            runSql(spark, singleSql);
        }
        long endTime = System.currentTimeMillis();
        logger.info("SQL execution time: {}ms", endTime - startTime);
        spark.sql("SHOW CATALOGS").show(false);
        spark.stop();
    }

    public static void runSql(SparkSession spark, String sql) {
        QueryExecution execution = spark.sql(sql).queryExecution();
        logger.debug(execution.explainString(ExplainMode.fromString(ExtendedMode.name())));
        List<String> res = JavaConverters.seqAsJavaList(HiveResult.hiveResultString(execution.executedPlan()));
        Schema tableSchema = getResultSetSchema(execution);
        System.out.println(
                tableSchema.getFieldSchemas().stream()
                        .map(FieldSchema::getName)
                        .collect(Collectors.joining("\t"))
        );
        res.forEach(System.out::println);
    }

    public static Schema getResultSetSchema(QueryExecution query) {
        LogicalPlan analyzed = query.analyzed();
        if (analyzed.output().isEmpty()) {
            return new Schema(Collections.singletonList(new FieldSchema("Response code", "string", "")), null);
        } else {
            List<FieldSchema> fieldSchemas = new ArrayList<>();
            for (Attribute attr : JavaConverters.seqAsJavaList(analyzed.output())) {
                fieldSchemas.add(new FieldSchema(
                        attr.name(),
                        attr.dataType().catalogString(), ""
                ));
            }
            return new Schema(fieldSchemas, null);
        }
    }

    public static void beautySql(String sql) {
        logger.info("\n---------------------- Running SQL ----------------------\n "
                + sql + "\n--------------------------------------------------------- ");
    }

    public static class InputParams {
        private List<DataSource> datasources;
        private List<UDFInfo> udfs;
        private String sql;

        public List<UDFInfo> getUdfs() {
            return udfs;
        }

        public List<DataSource> getDatasources() {
            return datasources;
        }

        public String getSql() {
            return sql;
        }
    }

    public static class UDFInfo {
        private String funcName;
        private String className;

        public String getFuncName() {
            return funcName;
        }

        public String getClassName() {
            return className;
        }
    }

    public static class DataSource {
        private String catalogName;
        private String password;
        private String url;
        private String user;

        public String getCatalogName() {
            return catalogName;
        }

        public String getPassword() {
            return password;
        }

        public String getUrl() {
            return url;
        }

        public String getUser() {
            return user;
        }
    }
}