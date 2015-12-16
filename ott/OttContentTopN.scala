package com.ctvit.ott

import java.sql.{DriverManager, Connection}
import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Date}

import com.ctvit.{MysqlFlag, AllConfigs}
import net.sf.json.JSONObject
import org.apache.spark.{SparkContext, SparkConf}
import redis.clients.jedis.Jedis
import scopt.OptionParser

import scala.collection.mutable

/**
 * Created by BaiLu on 2015/10/15.
 */
object OttContentTopN {

  private case class Params(
                             recNumber: Int = 20,
                             timeSpan: Int = 60,
                             taskId: String = null
                             )

  /**
   * mysql配置信息
   **/
  val configs = new AllConfigs
  val MYSQL_HOST = configs.OTT_MYSQL_HOST
  val MYSQL_PORT = configs.OTT_MYSQL_PORT
  val MYSQL_DB = configs.OTT_MYSQL_DB
  val MYSQL_DB_USER = configs.OTT_MYSQL_DB_USER
  val MYSQL_DB_PASSWD = configs.OTT_MYSQL_DB_PASSWD

  val MYSQL_CONNECT = "jdbc:mysql://" + MYSQL_HOST + ":" + MYSQL_PORT + "/" + MYSQL_DB
  val MYSQL_DRIVER = "com.mysql.jdbc.Driver"
  val MYSQL_QUERY = "select MovieID,CatalogID,MovieName from ottelementinfo;"
  /**
   * redis配置信息
   **/

  val REDIS_IP = configs.OTT_REDIS_IP
  val REDIS_PORT = configs.OTT_REDIS_PORT


  def main(args: Array[String]) {

    val defaultParams = Params()
    val mysqlFlag = new MysqlFlag
    val startTime = System.nanoTime()
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val parser = new OptionParser[Params]("BehaviorRecParams") {
      head("OTTBehaviorRecTonNProduct: an example Recommendation app for plain text data. ")
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
    val conf = new SparkConf().setAppName("OttContentTopN")
    val sc = new SparkContext(conf)

    /**
     * 读取配置文件信息，之前的参数采用配置文件的方式获取
     **/
    val timespan = timeSpans(params.timeSpan)
    val HDFS_DIR = s"hdfs://172.16.141.215:8020/data/ire/source/rec/ott/play/{$timespan}.csv"
    val mapcatalogid = mapCatalogID(MYSQL_QUERY)
    val rawRdd = sc.textFile(HDFS_DIR)
    val rdd = rawRdd
      .filter { line => val field = line.split(","); field(5).equals("1") && field(4) != ""}
      .map { line => val field = line.split(","); (field(3), 1)}
      .filter(tup => tup._1.matches("\\d+"))
      .map { tup => ((tup._1, mapcatalogid.getOrElse(tup._1, " ")), 1)}
      .filter(tup => tup._1._2.length > 2)
      .map { tup => ((tup._1._1, tup._1._2.toString.split("#")(0), tup._1._2.toString.split("#")(1)), 1)}
      //生成((MovieID,catalogid,MovieName),count)
      .reduceByKey(_ + _)

      /**
       * 返回值为(catalogID,(MovieID,MovieName,viewcount))
       **/
      .map(tup => (tup._1._2, (tup._1._1, tup._1._3, tup._2)))
      .groupByKey()
      .map { tup => (tup._1, sortByViewcountTopK(tup._2, params.recNumber))}
      .foreach(tup => insertRedis(tup._1, tup._2))
    sc.stop()
  }


  /**
   * 对于groupby后返回的iterable（contentid，contentname,viewcount），按照viewcount排序
   **/
  def sortByViewcountTopK(iterable: Iterable[(String, String, Int)], topK: Int): String = {
    val list = iterable.toList
    val sortlist = list.sortBy(_._3.toInt).reverse //升序排列返回结果
    var rec = ""
    val reclength = list.length
    var i = 0
    if (reclength >= topK) {
      while (i < topK) {
        rec += sortlist(i)._1 + "," + sortlist(i)._2 + "," + sortlist(i)._3 + "#"
        i += 1
      }
      rec
    }
    else {
      while (i < reclength) {
        rec += sortlist(i)._1 + "," + sortlist(i)._2 + "," + sortlist(i)._3 + "#"
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

  def initRedis(redisip: String, redisport: Int): Jedis = {
    val jedis = new Jedis(redisip, redisport, 100000)
    jedis
  }

  def initMySQL(): Connection = {
    Class.forName(MYSQL_DRIVER)
    DriverManager.getConnection(MYSQL_CONNECT, MYSQL_DB_USER, MYSQL_DB_PASSWD)
  }


  def mapCatalogID(sql: String): mutable.HashMap[String, String] = {
    val map = new mutable.HashMap[String, String]()
    val init = initMySQL()
    val rs = init.createStatement().executeQuery(sql)
    while (rs.next()) {
      val key = rs.getString(1)
      val value = rs.getString(2) + "#" + rs.getString(3)
      if (key.length > 0 && value.length > 2) {
        map.put(key, value)
      }
    }
    map
  }


  def insertRedis(catalogid: String, recItemList: String): Unit = {
    val jedis = initRedis(REDIS_IP, REDIS_PORT)
    val pipeline = jedis.pipelined()

    val map = new util.HashMap[String, String]()
    val arr = recItemList.split("#")
    /**
     * 0表示不加入类型seriestype
     * 1表示加入seriestype分电视剧与电影
     **/
    val key = "Topcontentlist_3_" + catalogid + "_0"
    val keynum = jedis.llen(key).toInt
    if (arr.length >= keynum) {
      var i = 0
      while (i < arr.length) {
        val recAssetId = ""
        val recAssetName = arr(i).split(",")(1)
        val recAssetPic = ""
        val recContentId = arr(i).split(",")(0)
        val recProviderId = (i + 1).toString
        val rank = arr(i).split(",")(2)
        map.put("assetId", recAssetId)
        map.put("assetname", recAssetName)
        map.put("assetpic", recAssetPic)
        map.put("movieID", recContentId)
        map.put("providerId", recProviderId)
        map.put("rank", rank)
        val value = JSONObject.fromObject(map).toString
        pipeline.rpush(key, value)
        i += 1
      }
      for (j <- 0 until keynum) {
        pipeline.lpop(key)
      }
      pipeline.sync()
    }
    jedis.disconnect()

  }


}
