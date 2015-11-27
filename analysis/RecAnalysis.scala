package com.ctvit.analysis

import java.sql.{DriverManager, Connection}
import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Calendar}

import com.ctvit.{MysqlFlag, AllConfigs}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser


import scala.collection.mutable
/**
 * Created by BaiLu on 2015/11/23.
 */
object RecAnalysis {

  private case class Params(
                             timeSpan: Int = 365
                             )

  /**
   * mysql配置信息
   **/
  val configs = new AllConfigs
  val MYSQL_HOST = configs.BOX_MYSQL_HOST
  val MYSQL_PORT = configs.BOX_MYSQL_PORT
  val MYSQL_DB = configs.BOX_MYSQL_DB
  val MYSQL_DB_USER = configs.BOX_MYSQL_DB_USER
  val MYSQL_DB_PASSWD = configs.BOX_MYSQL_DB_PASSWD

  val MYSQL_CONNECT = "jdbc:mysql://" + MYSQL_HOST + ":" + MYSQL_PORT + "/" + MYSQL_DB
  val MYSQL_DRIVER = "com.mysql.jdbc.Driver"
  val MYSQL_QUERY = "select id,sort_index from catalog_info where type=1 and sort_index is not null;"


  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("BehaviorRecParams") {
      head("RecAnalysis: an example analysis app for recommendation data. ")
      opt[Int]("timeSpan")
        .text(s"number of lambda, default: ${defaultParams.timeSpan}")
        .action((x, c) => c.copy(timeSpan = x))
    }
    parser.parse(args, defaultParams).map { params =>
      run(params)
    }.getOrElse {
      parser.showUsageAsError
      sys.exit(1)
    }


  }

  def run(params: Params) {

    val conf = new SparkConf().setAppName("RecommendatonAnalysis")
    val sc = new SparkContext(conf)

    val timespan = timeSpans(params.timeSpan)
    val HDFS_DIR = s"hdfs://172.16.141.215:8020/data/ire/source/rec/xor/vod/{$timespan}.csv"
    val VIEW_HDFS_DIR = "hdfs://172.16.141.215:8020/data/ire/result/rec/watchcount/"
    val map = mapSingleCid(MYSQL_QUERY)
    val rawRdd = sc.textFile(HDFS_DIR)
    val tripleRdd = rawRdd.map { line => val field = line.split(","); (field(11), field(0), field(5))}
      .filter(tup => tup._1.matches("\\d+"))
      .filter(tup => tup._2.matches("\\d+"))

      /**
       * 观看时长超过30秒的就计算
       **/
//      .filter(tup => tup._3.toInt > 30)

      /**
       * 对于单集电视剧将其映射为电视剧的catalogid
       **/
      .filter(tup => map.containsKey(tup._2))
      .map(tup => ((tup._1, map.get(tup._2)), tup._2))
    .groupByKey(40)
    .map(tup=>(tup._1._1,tup._1._2,watchCount(tup._2)))
      //userid,catalogid,count,watchcount,watchcount/count
    .map(tup=>tup._1+","+tup._2.split("#")(0)+","+tup._2.split("#")(1)+","+tup._3+","+1.0*tup._3/tup._2.split("#")(1).toInt)


    /*对于一集点击一次做统计计数*/
//      .map(tup => ((tup._1, map.get(tup._2)), 1))
//      .reduceByKey(_ + _, 35)
      //userid,catalogid,count,viewcount,viewcount/count
//      .map(tup => (tup._1._1, tup._1._2.split("#")(0), tup._1._2.split("#")(1), tup._2))
//      .map(tup => tup._1 + "," + tup._2 + "," + tup._3 + "," + tup._4 + "," + 1.0 * tup._4.toInt / tup._3.toInt)

    val hadoopconf = new Configuration()
    val fs = FileSystem.get(hadoopconf)
    val path = new Path(VIEW_HDFS_DIR)
    if (fs.exists(path)) {
      fs.delete(path, true)
      tripleRdd.saveAsTextFile(VIEW_HDFS_DIR)
    }
    else
      tripleRdd.saveAsTextFile(VIEW_HDFS_DIR)
    sc.stop()
  }


  /**
   * 设置读取数据文件的时间间隔,用时间天作为间隔参数
   **/

  def timeSpans(span: Int): String = {
    val df = new SimpleDateFormat("yyyyMMdd")
    val buffer = new StringBuffer()
    for (i <- 1 to span) {
      val canlendar = Calendar.getInstance()
      canlendar.add(Calendar.DAY_OF_YEAR, -i)
      buffer.append(df.format(canlendar.getTime) + ",")
    }
    buffer.toString
  }

  def initMySQL(): Connection = {
    Class.forName(MYSQL_DRIVER)
    DriverManager.getConnection(MYSQL_CONNECT, MYSQL_DB_USER, MYSQL_DB_PASSWD)
  }

  def mapSingleCid(sql: String): util.HashMap[String, String] = {
    val map = new util.HashMap[String, String]()
    val init = initMySQL()
    val rs = init.createStatement().executeQuery(sql)
    while (rs.next()) {
      val catalogid = rs.getString(1)
      val arr = rs.getString(2).split(";")
      if (arr.length > 1) {
        for (i <- 0 until arr.length) {
          map.put(arr(i), catalogid + "#" + arr.length)
        }
      }
      else
        map.put(rs.getString(2), catalogid + "#" + arr.length)
    }
    map
  }
  def watchCount(iterable: Iterable[(String)]): Int ={
    val list=iterable.toList
    val len=list.length
    val set=new mutable.HashSet[String]()
    for(i<-0 until len){
      set.add(list(i))
    }
    set.size
  }


}
