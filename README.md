# spark-sql-for-cluster
Spark Sql on Yarn Cluster or Kubeflow-Spark-Operator 


## Kubeflow-Spark-Operator Spark-Sql Task 示例
Kubeflow-Spark-Operator 目前还没有对sql的支持，所以这个项目可以作为Spark-Operator下的sql client使用，支持多数据源，如下示例中的mysql+pg

输入参数为一个大JSON：
```

{
    "datasources": [
        {
            "catalogName": "db1",
            "password": "xxx",
            "user": "root",
            "url": "jdbc:mysql://ip:port/db1"
        },
        {
            "catalogName": "db2",
            "password": "xxx",
            "user": "postgres",
            "url": "jdbc:postgresql://ip:port/db2"
        }
    ],
    "udfs": [
        {
            "funcName": "ip_to_add",
            "className": "org.example.IPtoAddress"
        }
    ],
    "sql": "insert into db1.table1 from select client_ip, ip_to_add(client_ip) from db2.table2"
}
```
spark-sql.jar即为本项目打包后的jar，需要提前打入到镜像中或者放在共享存储上
```
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: spark-sql-task-152864546134816-1-333-297
  namespace: default
spec:
  type: Scala
  mode: cluster
  sparkVersion: "3.5.5"
  image: spark:3.5.5
  imagePullPolicy: IfNotPresent
  timeToLiveSeconds: 86400
  mainClass: org.apache.spark.sql.clients.SparkSQLCli
  mainApplicationFile: local:///opt/spark-sql/jas/spark-sql.jar # spark-sql client jar stored in the image
  arguments:
    - |
        {"datasources":[{"catalogName":"db1","password":"xxx","user":"root","url":"jdbc:mysql://ip:port/db1"},{"catalogName":"db2","password":"xxx","user":"postgres","url":"jdbc:postgresql://ip:port/db2"}],"udfs":[{"funcName":"ip_to_add","className":"org.example.IPtoAddress"}],"sql":"insert into db1.table1 from select client_ip, ip_to_add(client_ip) from db2.table2"}
  restartPolicy:
    type: Never

  sparkConf:
    spark.eventLog.enabled: "false"
    spark.eventLog.dir: "/tmp"
  deps:
    jars: [hdfs://localhost:8020/udfs/udf.jar]
  volumes:
    - name: datas
      hostPath:
        path: /tmp/spark_dep_jars

  driver:
    cores: 1
    memory: 1g
    serviceAccount: spark-account

  executor:
    cores: 1
    instances: 1
    memory: 1g

```
