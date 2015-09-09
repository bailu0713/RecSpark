package com.ctvit.box

import java.sql.{ResultSet, DriverManager, Connection}
import java.text.SimpleDateFormat
import java.util
import java.util.Date

import net.sf.json.JSONObject
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkContext, SparkConf}
import redis.clients.jedis.Jedis
import scopt.OptionParser

/**
 * Created by BaiLu on 2015/9/6.
 */
object NewContent {

  /**
   * mysql配置信息
   **/
  val MYSQL_HOST = ""
  val MYSQL_PORT = ""
  val MYSQL_DB = ""
  val MYSQL_DB_USER = ""
  val MYSQL_DB_PASSWD = ""
  val MYSQL_CONNECT = "jdbc:mysql://" + MYSQL_HOST + ":" + MYSQL_PORT + "/" + MYSQL_DB
  val MYSQL_DRIVER = "com.mysql.jdbc.Driver"

  /**
   * redis配置信息
   **/
  val REDIS_IP = ""
  val REDIS_PORT = 6379

  private case class Params(
                             recNumber: Int = 25,
                             taskId: String = null
                             )

  def main(args: Array[String]) {

    val startTime = System.nanoTime()
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val defaultParams = Params()
    val mysqlFlag = new MysqlFlag
    val parser = new OptionParser[Params]("ContentRecParams") {
      head("set ContentRecParams")
      opt[Int]("recNumber")
        .text(s"the number of reclist default:${defaultParams.recNumber}")
        .action((x, c) => c.copy(recNumber = x))
      opt[String]("taskId")
        .text("the mysql flag key")
        .required()
        .action((x, c) => c.copy(taskId = x))
    }
    try {
      parser.parse(args, defaultParams)
        .map { params =>
        run(params)
        val endTime = df.format(new Date(System.currentTimeMillis()))
        val period = ((System.nanoTime() - startTime) / 1e9).toString
        mysqlFlag.runSuccess(params.taskId, endTime, period)
      }.getOrElse {
        parser.showUsageAsError
        sys.exit(-1)
      }
    } catch {
      case _: Exception =>
        val errTime = df.format(new Date(System.currentTimeMillis()))
        parser.parse(args, defaultParams).map { params =>
          mysqlFlag.runFail(params.taskId, errTime)
        }
    }
  }

  def run(params: Params) {
    val conf = new SparkConf().setAppName("NewContent")
    val sc = new SparkContext(conf)
    val allRdd = new JdbcRDD(sc, initMySQL, s"select contentId, contentName,seriesType from ire_content_relation where contentId >=? and contentId <= ? ORDER BY resource_time DESC limit ${params.recNumber};", 1, 2000000000, 1, extractValues)
      .distinct()
      .keyBy(tup => tup._2)
      .groupByKey()
      .foreach(tup => insertRedis(tup._2))


    val levelIdArr = catalogLeveId().size()
    for (i <- 0 until levelIdArr) {
      val levelId = catalogLeveId().get(i)
      val catalogRdd = new JdbcRDD(sc, initMySQL, s"select contentId, contentName,seriesType from ire_content_relation where contentId >=? and contentId <= ? and (level1Id = '$levelId' or level2Id = '$levelId' or level3Id = '$levelId' or level4Id = '$levelId' or level5Id = '$levelId' or level6Id = '$levelId') ORDER BY resource_time DESC limit ${params.recNumber};", 1, 2000000000, 1, extractValues)
        .distinct()
        .keyBy(tup => tup._2)
        .groupByKey()
        .foreach(tup => insertCatalogRedis(tup._2, levelId))
    }
  }


  def initRedis(redisip: String, redisport: Int): Jedis = {
    val jedis = new Jedis(redisip, redisport)
    jedis
  }

  def initMySQL(): Connection = {
    Class.forName(MYSQL_DRIVER)
    DriverManager.getConnection(MYSQL_CONNECT, MYSQL_DB_USER, MYSQL_DB_PASSWD)
  }

  def catalogLeveId(): util.ArrayList[String] = {

    val arr = new util.ArrayList[String]
    for (i <- 1 to 6) {
      val sql = s"select DISTINCT(level${i}Id) from ire_content_relation;"
      val init = initMySQL()
      val rs = init.createStatement().executeQuery(sql)
      while (rs.next()) {
        arr.add(rs.getString(1))
      }
    }
    arr
  }

  def extractValues(r: ResultSet) = {
    (r.getString(1), r.getString(2), r.getString(3))
  }


  def insertRedis(iterable: Iterable[(String, String, String)]): Unit = {
    //插入一条删除之前的一条
    val list = iterable.toList
    val jedis = initRedis(REDIS_IP, REDIS_PORT)
    val map = new util.HashMap[String, String]()
    val key = "Newcontentlist_5_0_" + list(0)._3
    val keynum = jedis.llen(key).toInt
    val recAssetId = ""
    val recAssetPic = ""
    val recProviderId = ""
    val rank = ""
    map.put("assetId", recAssetId)
    map.put("assetname", list(0)._2)
    map.put("assetpic", recAssetPic)
    map.put("movieID", list(0)._1)
    map.put("providerId", recProviderId)
    map.put("rank", rank)
    val value = JSONObject.fromObject(map).toString
    jedis.rpush(key, value)
        jedis.lpop(key)
    //    for (j <- 0 until keynum) {
    //      jedis.lpop(key)
    //    }
    jedis.disconnect()
  }

  def insertCatalogRedis(iterable: Iterable[(String, String, String)], levelId: String): Unit = {
    //插入一条删除之前的一条
    val list = iterable.toList
    val jedis = initRedis(REDIS_IP, REDIS_PORT)
    val map = new util.HashMap[String, String]()
    val key = "Newcontentlist_5_" + levelId + "_" + list(0)._3
    val keynum = jedis.llen(key).toInt
    val recAssetId = ""
    val recAssetPic = ""
    val recProviderId = ""
    val rank = ""
    map.put("assetId", recAssetId)
    map.put("assetname", list(0)._2)
    map.put("assetpic", recAssetPic)
    map.put("movieID", list(0)._1)
    map.put("providerId", recProviderId)
    map.put("rank", rank)
    val value = JSONObject.fromObject(map).toString
    jedis.rpush(key, value)
        jedis.lpop(key)
    //    for (j <- 0 until keynum) {
    //      jedis.lpop(key)
    //    }
    jedis.disconnect()
  }

}
