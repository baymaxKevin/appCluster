package com.appCluster.constants

/**
  * @Author: JZ.lee
  * @Description: TODO
  * @Date: 19-3-29 下午2:30
  * @Modified By:
  */
object Constants {
  val FILTER = "hdfs://lee:8020/appCluster/filter"
  val OPZK = "hdfs://lee:8020/appCluster/opzK"
  val SAMPLE = "hdfs://lee:8020/appCluster/data/*train*"
  val PIPES = "hdfs://lee:8020/appCluster/model/pipes"
  val CLUSTER = "hdfs://lee:8020/appCluster/model/cluster"
  val ONLINE = "hdfs://lee:8020/appCluster/*test*"
  val PREDICTION = "hdfs://lee:8020/appCluster/prediction/"

  val KAFKA_BJ_ECS = "lee:9092"
}
