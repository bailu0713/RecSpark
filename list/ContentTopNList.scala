package com.ctvit.list

/**
 * Created by BaiLu on 2015/11/10.
 */

import java.sql.{Connection, DriverManager}
import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Date}

import com.ctvit.{AllConfigs, MysqlFlag}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

object ContentTopNList {

  private case class Params(
                             recNumber: Int = 15,
                             timeSpan: Int = 30,
                             taskId: String = null
                             )

  val configs = new AllConfigs
  val MYSQL_HOST = configs.BOX_MYSQL_HOST
  val MYSQL_PORT = configs.BOX_MYSQL_PORT
  val MYSQL_DB = configs.BOX_MYSQL_DB
  val MYSQL_DB_USER = configs.BOX_MYSQL_DB_USER
  val MYSQL_DB_PASSWD = configs.BOX_MYSQL_DB_PASSWD

  val MYSQL_CONNECT = "jdbc:mysql://" + MYSQL_HOST + ":" + MYSQL_PORT + "/" + MYSQL_DB
  val MYSQL_DRIVER = "com.mysql.jdbc.Driver"
  val MYSQL_QUERY = "select catalog_info.id,catalog_info.sort_index from ire_content_relation inner join catalog_info on ire_content_relation.contentId=catalog_info.id where catalog_info.type=1 and sort_index is not null;"
  val MYSQL_QUERY_LEVELIDNAME = "select contentId,level1Id from ire_content_relation;"

  def main(args: Array[String]) {

    val defaultParams = Params()
    val mysqlFlag = new MysqlFlag
    val startTime = System.nanoTime()
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val parser = new OptionParser[Params]("ContentTopNRecParams") {
      head("ChannelTopNProduct: an example Recommendation app for plain text data. ")
      opt[Int]("recNumber")
        .text(s"number of lambda, default: ${defaultParams.recNumber}")
        .action((x, c) => c.copy(recNumber = x))
      opt[Int]("timeSpan")
        .text(s"number of lambda, default: ${defaultParams.timeSpan}")
        .action((x, c) => c.copy(timeSpan = x))
      opt[String]("taskId")
        .text("set mysql flag")
        .unbounded()
        .required()
        .action((x, c) => c.copy(taskId = x))
    }
    try {
      parser.parse(args, defaultParams).map { params =>
        run(params)
        val period = ((System.nanoTime() - startTime) / 1e6).toString.split("\\.")(0)
        val endTime = df.format(new Date(System.currentTimeMillis()))
        mysqlFlag.runSuccess(params.taskId, endTime, period)
      }.getOrElse {
        parser.showUsageAsError
        sys.exit(1)
      }
    } catch {
      case _: Exception =>
        val errTime = df.format(new Date(System.currentTimeMillis()))
        parser.parse(args, defaultParams).map { params =>
          mysqlFlag.runFail(params.taskId, errTime)
        }
    }
  }

  private def run(params: Params) {
    val conf = new SparkConf().setAppName("ContentTopN")
    val sc = new SparkContext(conf)
    val timespan = timeSpans(params.timeSpan)
    val df = new SimpleDateFormat("yyyyMMdd")
    val today = df.format(new Date())
    val HDFS_DIR_RECLIST = s"hdfs://172.16.141.215:8020/data/ire/result/rec/topn/$today"
    val HDFS_DIR = s"hdfs://172.16.141.215:8020/data/ire/source/rec/xor/vod/{$timespan}.csv"
    val maplevelidname = maplevelIdName(MYSQL_QUERY_LEVELIDNAME)
    val mapcidcount = mapSingleCidCount(MYSQL_QUERY)
    val rawRdd = sc.textFile(HDFS_DIR)

    val rdd = rawRdd.map { line => val field = line.split(","); (field(5), field(0))}
      .filter(tup => tup._1.matches("\\d+"))
      .filter(tup => tup._2.matches("\\d+"))
      .filter(tup => tup._1.toInt > 60)

      .map {
      tup => if (mapcidcount.containsKey(tup._2)) ((mapcidcount.get(tup._2).split("#")(0), 1, 1), 1)
      else ((tup._2, 1, 0), 1)
    }
      //生成((contentid,seriestype),count)
      .reduceByKey(_ + _, 15)

      /**
       * 返回值为(contentId,seriestype,viewcount)
       **/
      .map(tup => (tup._1._1, tup._1._3, tup._2 * 1.0 / tup._1._2))
    /**
     * 返回值为(contentId,seriestype,viewcount,levelId)
     *
     **/
    val finalrdd = rdd
      .map { tup => (tup._1, maplevelidname.get(tup._1), tup._2, tup._3)}
      .filter(tup => tup._2 != null)
      .map { tup =>
      //(contentid，seriestype,viewcount,level1id)
      (tup._1, tup._3, tup._4, tup._2)
    }

    /**
     * level1id插入redis
     * seriestype没有考虑
     **/
    val finalrdds = finalrdd
      .filter(tup => tup._4 != "0")
      .groupBy(tup => tup._4)
      .repartition(5)
      .map { tup =>
      ("Topcontentlist_5_" + tup._1, sortByViewcountTopK(tup._2, params.recNumber))
    }

    val hadoopconf = new Configuration()
    val fs = FileSystem.get(hadoopconf)
    val path = new Path(HDFS_DIR_RECLIST)
    if (fs.exists(path)) {
      fs.delete(path, true)
      finalrdds.saveAsTextFile(HDFS_DIR_RECLIST)
    }
    else
      finalrdds.saveAsTextFile(HDFS_DIR_RECLIST)
    sc.stop()
  }

  /**
   * 对于groupby后返回的iterable（contentid,seriestype,viewcount,level1id），按照viewcount排序
   **/
  def sortByViewcountTopK(iterable: Iterable[(String, Int, Double, String)], topK: Int): String = {
    val list = iterable.toList
    val sortlist = list.sortBy(_._3).reverse //升序排列返回结果
    var rec = ""
    val reclength = list.length
    var i = 0
    if (reclength >= topK) {
      while (i < topK) {
        rec += sortlist(i)._1 + "#"
        i += 1
      }
      rec
    }
    else {
      while (i < reclength) {
        rec += sortlist(i)._1 + "#"
        i += 1
      }
      rec
    }

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

  /**
   * 将电视剧系列的sortindex每一项作为key，catalogid与对应的集数作为value即(catalogid,count(电视剧的集数))
   * 保证只读取一次mysql，不要每次去读取
   **/
  def mapSingleCidCount(sql: String): util.HashMap[String, String] = {
    //    catalog_info.id,catalog_info.sort_index
    val map = new util.HashMap[String, String]()
    val init = initMySQL()
    val rs = init.createStatement().executeQuery(sql)
    while (rs.next()) {
      val arr = rs.getString(2).split(";")
      val value = rs.getString(1) + "#" + arr.length.toString
      if (arr.length > 1) {
        for (i <- 0 until arr.length) {
          map.put(arr(i), value)
        }
      }
      else
        map.put(rs.getString(2), value)
    }
    map
  }

  /**
   * 将每一个contentid映射出他的各个level的id和levelname (contentid,contntName#level1Id#level1Name#level2Id#levele2Name...)
   * 保证只读取一次mysql，不要每次去读取
   **/
  def maplevelIdName(sql: String): util.HashMap[String, String] = {
    val map = new util.HashMap[String, String]()
    val init = initMySQL()
    val rs = init.createStatement().executeQuery(sql)
    while (rs.next()) {
      //contentid作为key
      if (rs.getString(2) != "10019864") {
        val key = rs.getString(1)
        val value = rs.getString(2)
        map.put(key, value)
      }
    }
    map
  }

  def catalogLevelId(): util.ArrayList[String] = {
    val arr = new util.ArrayList[String]
    val sql = s"select DISTINCT(level1Id) from ire_content_relation;"
    val init = initMySQL()
    val rs = init.createStatement().executeQuery(sql)
    while (rs.next()) {
      arr.add(rs.getString(1))
    }
    arr
  }

}

