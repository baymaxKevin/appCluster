package com.appCluster.cluster

import com.appCluster.cluster.AppCluster.AppList
import com.appCluster.constants.Constants
import com.appCluster.utils.HBaseUtils
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.{Bytes, MD5Hash}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.sql.SparkSession
/**
  * @Author: JZ.lee
  * @Description: TODO
  * @Date: 19-3-29 下午4:10
  * @Modified By:
  */
object ClusterPrediction {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    var numPartitions=10
    if(args.length>0) numPartitions=args(0).toInt

    val spark = SparkSession.builder()
      .appName("OnLineCluster")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import spark.implicits._
    // 加载textFile
    val filter_vocabulary = spark.sparkContext.textFile(Constants.FILTER).collect().mkString(",")
    println("过滤词表：" + filter_vocabulary)
    val df = spark.read.textFile(Constants.ONLINE ).map(row=>{
      val rows = row.split(",")
      AppList(rows(0).toLong,rows(1).split(";").filter(!filter_vocabulary.split(",").contains(_)))
    }).toDF("uid","app_list").repartition(numPartitions)

    // app_list列转化为稀疏特征向量
    val pips = PipelineModel.load(Constants.PIPES)
    val result = pips.transform(df).select("uid","app_list_idf")

    val kmm = KMeansModel
      .load(Constants.CLUSTER)
      .setFeaturesCol("app_list_idf")
      .setPredictionCol("prediction")
    val kc = kmm.transform(result).select("uid","prediction")

    val rdd1 = kc.rdd
    rdd1.saveAsTextFile(Constants.PREDICTION )

    rdd1.map(row=>{
      val uid = row.getAs[Long]("uid").toString
      val rowKey = MD5Hash.getMD5AsHex(Bytes.toBytes(uid)).substring(0,8) + "_" + uid
      val prediction = row.getAs[Int]("prediction").toString
      val cols = Array(prediction)
      (new ImmutableBytesWritable, HBaseUtils.getPutAction(rowKey, "p", Array("appLTag"), cols))
    }).saveAsHadoopDataset(HBaseUtils.getJobConf("user_app_cluster"))

    spark.stop()
  }
}
