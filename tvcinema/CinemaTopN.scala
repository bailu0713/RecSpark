package com.ctvit.tvcinema

/**
 * Created by BaiLu on 2015/12/16.
 */

import java.sql.{DriverManager, Connection}
import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Calendar}

import com.ctvit.{AllConfigs, MysqlFlag}
import net.sf.json.JSONObject
import org.apache.spark.{SparkContext, SparkConf}
import redis.clients.jedis.Jedis
import scopt.OptionParser

/**
 * Created by BaiLu on 2015/9/6.
 */
object CinemaTopN {

  private case class Params(
                             recNumber: Int = 20,
                             timeSpan: Int = 30,
                             taskId: String = null
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
  val MYSQL_QUERYS = "select id,name from element_info where genre not like '%11%' and genre not like '%20%' and genre not like '%41%';"
  /**
   * redis配置信息
   **/

  val REDIS_IP = configs.BOX_REDIS_IP
  val REDIS_IP2 = configs.BOX_REDIS_IP2
  val REDIS_PORT = configs.BOX_REDIS_PORT


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
    val HDFS_DIR = s"hdfs://172.16.141.215:8020/data/ire/source/rec/vsp/vod/{$timespan}.csv"

    val mapelementid = mapElementId(MYSQL_QUERYS)
    val rawRdd = sc.textFile(HDFS_DIR)

    /**
     * user,contenet,key_code
     **/
    val rdd = rawRdd.map { line => val field = line.split(","); (field(6), field(5), field(7))}
      .filter(tup => tup._1.matches("\\d+"))
      .filter(tup => tup._2.matches("\\d+"))
      .filter(tup => tup._3 == "ahqhse8x8yr8ex")

      /**
       * 电影生成（contentid,name,seriestype,1(1次观看)）
       **/
      .map {
      tup => if (mapelementid.containsKey(tup._2)) ((tup._2, mapelementid.get(tup._2), 0), 1)
      else (("no", "name", 0), 1)
    }
      .filter(_._1._1 != "no")
      //生成((contentid,name,seriestype),count)
      .reduceByKey(_ + _)

      /**
       * 返回值为(contentId,seriestype,viewcount,name)
       **/
      .map(tup => (tup._1._1, tup._1._3, tup._2, tup._1._2))

    val finalrdd_series = rdd.sortBy(tup => tup._3, false, 15)
    if (finalrdd_series.count() > params.recNumber)
      finalrdd_series
        .take(params.recNumber)
        .groupBy(tup => tup._2)
        .foreach(tup => insertRedis(tup._2))

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

  def initRedis(redisip: String, redisport: Int): Jedis = {
    val jedis = new Jedis(redisip, redisport, 100000)
    jedis
  }

  def initMySQL(): Connection = {
    Class.forName(MYSQL_DRIVER)
    DriverManager.getConnection(MYSQL_CONNECT, MYSQL_DB_USER, MYSQL_DB_PASSWD)
  }

  def mapElementId(sql: String): util.HashMap[String, String] = {
    val map = new util.HashMap[String, String]()
    val init = initMySQL()
    val rs = init.createStatement().executeQuery(sql)
    while (rs.next()) {
      map.put(rs.getString(1), rs.getString(2))
    }
    map
  }


  def insertRedis(arr: Array[(String, Int, Int, String)]): Unit = {

    //去名字相同的电影
    for (i <- 0 until arr.length) {
      for (j <- i + 1 until arr.length) {
        if (arr(i)._4 == arr(j)._4)
          arr.update(j, ("repeate", 1, 1, "repeate"))
      }
    }

    val jedis = initRedis(REDIS_IP, REDIS_PORT)
    val jedis2 = initRedis(REDIS_IP2, REDIS_PORT)

    val pipeline = jedis.pipelined()
    val pipeline2 = jedis2.pipelined()
    val map = new util.HashMap[String, String]()

    val key = "Topcontentlist_5_10001719"
    val keynum = jedis.llen(key).toInt
    val keynum2 = jedis2.llen(key).toInt
    if (arr.length > 5) {
      var i = 0
      while (i < arr.length) {
        if (arr(i)._4 != "repeate") {
          val recAssetId = ""
          val recAssetName = arr(i)._4
          val recAssetPic = ""
          val recContentId = arr(i)._1
          val recParentName = ""
          val recParentId = "10046284"
          val recProviderId = ""
          val rank = (i + 1).toString
          map.put("assetId", recAssetId)
          map.put("assetname", recAssetName)
          map.put("assetpic", recAssetPic)
          map.put("movieID", recContentId)
          map.put("parentName", recParentName)
          map.put("parentid", recParentId)
          map.put("providerId", recProviderId)
          map.put("rank", rank)
          val value = JSONObject.fromObject(map).toString
          pipeline.rpush(key, value)
          pipeline2.rpush(key, value)
          i += 1
        }
        else
          i += 1
      }
      for (j <- 0 until keynum) {
        pipeline.lpop(key)
      }
      pipeline.sync()
      for (j <- 0 until keynum2) {
        pipeline2.lpop(key)
      }
      pipeline2.sync()
    }
    jedis.disconnect()
    jedis2.disconnect()
  }


}

