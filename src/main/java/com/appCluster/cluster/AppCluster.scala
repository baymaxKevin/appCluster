package com.appCluster.cluster

import java.util

import com.appCluster.constants.Constants
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{HashingTF, IDF}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.SparkSession
/**
  * @Author: JZ.lee
  * @Description: TODO
  * @Date: 19-3-29 下午2:37
  * @Modified By: app列表->hash编号->idf->Kmeans->opz K->save model params
  */
object AppCluster {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    var numPartitions=2
    if(args.length>0) numPartitions=args(0).toInt
    var maxIter = 20
    if(args.length>1) maxIter=args(1).toInt
    var minSeq=128
    if(args.length>2) minSeq=args(2).toInt
    var maxSeq=129
    if(args.length>3) maxSeq=args(3).toInt

    val spark = SparkSession.builder()
      .appName("AppCluster")
      .master("local[2]")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val conf = new Configuration()
    // 防止伪分布式出现错误：java.lang.IllegalArgumentException: Wrong FS: hdfs://lee:8020/appCluster/opzK, expected
    conf.set("fs.defaultFS", "hdfs://lee:8020")
    val fs = FileSystem.get(conf)
    val out = if(fs.exists(new Path(Constants.OPZK))){
      fs.append(new Path(Constants.OPZK))
    }else {
      fs.create(new Path(Constants.OPZK))
    }

    import spark.implicits._
    // 加载textFile
    val filter_vocabulary = spark.sparkContext.textFile(Constants.FILTER).collect().mkString(",")
    println("过滤词表：" + filter_vocabulary)
    val df = spark.read.textFile(Constants.SAMPLE).map(row=>{
      val rows = row.split(",")
      AppList(rows(0).toLong,rows(1).split(";").filter(!filter_vocabulary.split(",").contains(_)))
    }).toDF("uid","app_list").repartition(numPartitions)
    df.show(10,false)

    // app_list列转化为稀疏特征向量
    var flag = false
    val pips = if(!fs.exists(new Path(Constants.PIPES))){
      flag = true
      val htf = new HashingTF().setInputCol("app_list").setOutputCol("app_list_htf").setNumFeatures(math.pow(2,18).toInt)
      val idf = new IDF().setInputCol("app_list_htf").setOutputCol("app_list_idf")
      new Pipeline().setStages(Array(htf,idf)).fit(df)
    }else{
      PipelineModel.load(Constants.PIPES)
    }
    if(flag){
      pips.write.overwrite().save(Constants.PIPES)
    }

    val result = pips.transform(df).select("uid","app_list_idf")
    result.cache()
    result.show(10,false)

    // result在Kmeans迭代寻优多次用到，因此加载内存
    val map = new util.HashMap[Int,Double]()
    // k-means聚类
    val ks = Range(minSeq, maxSeq)
    var minSsd = 0.0
    ks.foreach(cluster => {
      val kmeans = new KMeans()
        .setK(cluster)
        .setMaxIter(maxIter)
        .setFeaturesCol("app_list_idf")
        .setPredictionCol("prediction")
      val kmm = kmeans.fit(result)
      val ssd = kmm.computeCost(result)
      map.put(cluster, ssd)
      if(minSsd > 0){
        minSsd = if(minSsd > ssd) {
          kmm.write.overwrite().save(Constants.CLUSTER)
          ssd
        } else minSsd
      }else{
        kmm.write.overwrite().save(Constants.CLUSTER)
        minSsd = ssd
      }

      println("当k=" + cluster + "，点到最近中心距离的平方和:"+ ssd)
    })
    // 释放缓存
    result.unpersist()
    out.write(map.toString.getBytes("UTF-8"))
    spark.stop()
  }

  case class AppList(uid: Long, app_list: Array[String]) extends Serializable
}
