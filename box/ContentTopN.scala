/**
 * Created by BaiLu on 2015/12/14.
 */
package com.ctvit.box

import java.sql.{DriverManager, Connection}
import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Calendar}

import com.ctvit.AllConfigs
import com.ctvit.MysqlFlag
import com.ctvit.{AllConfigs, MysqlFlag}
import net.sf.json.JSONObject
import org.apache.spark.{SparkContext, SparkConf}
import redis.clients.jedis.Jedis
import scopt.OptionParser

/**
 * Created by BaiLu on 2015/9/6.
 */
object ContentTopN {

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
  val MYSQL_QUERY = "select id,sort_index,name from catalog_info where type=1 and sort_index is not null;"
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

    /**
     * 读取配置文件信息，之前的参数采用配置文件的方式获取
     *
     **/
    val timespan = timeSpans(params.timeSpan)
    val HDFS_DIR = s"hdfs://172.16.141.215:8020/data/ire/source/rec/vsp/vod/{$timespan}.csv"

    val mapcatalogid = mapSingleCidCount(MYSQL_QUERY)
    val mapelementid = mapElementId(MYSQL_QUERYS)
    val rawRdd = sc.textFile(HDFS_DIR)

    /**
     * user,contenet,key_code
     **/
    val rdd = rawRdd.map { line => val field = line.split(","); (field(6), field(5), field(7))}
      .filter(tup => tup._1.matches("\\d+"))
      .filter(tup => tup._2.matches("\\d+"))
      .filter(tup => tup._3 == "ahyuvzq53adjct")

      /**
       * 对于单集电视剧将其映射为电视剧的catalogid作为MovieId
       * 生成（contentid,contentidcount(总集数),seriestype(0,1)，name,1(1次观看)）
       * 电影则生成（contentid,1（总集数）,seriestype(0,1)，name,1(1次观看)）
       * /*mapcidcount.get(tup._2).split("#")(1).toInt*/
       **/
      .map {
      tup => if (mapcatalogid.containsKey(tup._2)) ((mapcatalogid.get(tup._2).split("#")(0), mapcatalogid.get(tup._2).split("#")(1).toInt, 1, mapcatalogid.get(tup._2).split("#")(2)), 1)
      else if (mapelementid.containsKey(tup._2)) ((tup._2, 1, 0, mapelementid.get(tup._2)), 1)
      else (("no", 1, 0, "name"), 1)
    }
      .filter(_._1._1 != "no")

      //生成((contentid,seriestype),count)
      .reduceByKey(_ + _)

      /**
       * 返回值为(contentId,seriestype,viewcount)
       **/
      .map(tup => (tup._1._1, tup._1._3, tup._2 * 1.0 / tup._1._2, tup._1._4))



    val finalrdd_series = rdd.filter(tup => tup._2 == 1).sortBy(tup => tup._3, false, 15)
    if (finalrdd_series.count() > params.recNumber)
      finalrdd_series
        .take(params.recNumber)
        .groupBy(tup => tup._2)
        .foreach(tup => insertRedis(tup._1, tup._2))


    val finalrdd_nonseries = rdd.filter(tup => tup._2 == 0).sortBy(tup => tup._3, false, 15)
    if (finalrdd_nonseries.count() > params.recNumber)
      finalrdd_nonseries
        .take(params.recNumber)
        .groupBy(tup => tup._2)
        .foreach(tup => insertRedis(tup._1, tup._2))

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

  def mapSingleCidCount(sql: String): util.HashMap[String, String] = {
    //    catalog_info.id,catalog_info.sort_index
    val map = new util.HashMap[String, String]()
    val init = initMySQL()
    val rs = init.createStatement().executeQuery(sql)
    while (rs.next()) {
      val arr = rs.getString(2).split(";")
      val value = rs.getString(1) + "#" + arr.length.toString + "#" + rs.getString(3)
      if (arr.length > 1) {
        for (i <- 0 until arr.length) {
          map.put(arr(i), value)
        }
      }
      else
        map.put(rs.getString(2), value)
    }
    init.close()
    map
  }


  def mapElementId(sql: String): util.HashMap[String, String] = {
    val map = new util.HashMap[String, String]()
    val init = initMySQL()
    val rs = init.createStatement().executeQuery(sql)
    while (rs.next()) {
      map.put(rs.getString(1), rs.getString(2))
    }
    init.close()
    map
  }


  def insertRedis(seriestype: Int, arr: Array[(String, Int, Double, String)]): Unit = {

    //去名字相同的电影
    for (i <- 0 until arr.length) {
      for (j <- i + 1 until arr.length) {
        if (arr(i)._4 == arr(j)._4)
          arr.update(j, ("repeate", 1, 1.0, "repeate"))
      }
    }

    val jedis = initRedis(REDIS_IP, REDIS_PORT)
    val jedis2 = initRedis(REDIS_IP2, REDIS_PORT)

    val pipeline = jedis.pipelined()
    val pipeline2 = jedis2.pipelined()

    val map = new util.HashMap[String, String]()
    /**
     * 0表示不加入类型seriestype
     * 1表示加入seriestype分电视剧与电影
     **/
    val key =
      if (seriestype == 0) "Topcontentlist_5_10046284"
      else "Topcontentlist_5_10046284" + "_" + seriestype

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
          val rank = (i+1).toString
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
