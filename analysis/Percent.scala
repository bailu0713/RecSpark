package com.ctvit.analysis

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by BaiLu on 2015/11/23.
 */
object Percent {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("RecommendatonAnalysis")
    val sc = new SparkContext(conf)
    val VIEW_HDFS_DIR = "hdfs://172.16.141.215:8020/data/ire/result/rec/watchcount/part-*"
    val rdd = sc.textFile(VIEW_HDFS_DIR)
    val count = rdd.count()
    val percent=rdd
      .map { line => val field = line.split(","); field(4)}
      .map(field => (casePercent(field.toDouble), 1))
      .reduceByKey(_ + _,1)
      .map(tup => tup._1 + "," + tup._2 + "," +count+","+ 1.0 * tup._2 / count)



    val PERCENT_HDFS_DIR = "hdfs://172.16.141.215:8020/data/ire/result/rec/percent/"
    val hadoopconf = new Configuration()
    val fs = FileSystem.get(hadoopconf)
    val path = new Path(PERCENT_HDFS_DIR)
    if (fs.exists(path)) {
      fs.delete(path, true)
      percent.saveAsTextFile(PERCENT_HDFS_DIR)
    }
    else
      percent.saveAsTextFile(PERCENT_HDFS_DIR)
    sc.stop()
  }





  def casePercent(percent: Double): Double = {
    if (percent >= 0.0 && percent <= 0.1)
      0.1
    else if (percent > 0.1 && percent <= 0.2)
      0.2
    else if (percent > 0.2 && percent <= 0.3)
      0.3
    else if (percent > 0.3 && percent <= 0.4)
      0.4
    else if (percent > 0.4 && percent <= 0.5)
      0.5
    else if (percent > 0.5 && percent <= 0.6)
      0.6
    else if (percent > 0.6 && percent <= 0.7)
      0.7
    else if (percent > 0.7 && percent <= 0.8)
      0.8
    else if (percent > 0.8 && percent <= 0.9)
      0.9
    else
      1.0
  }

}
