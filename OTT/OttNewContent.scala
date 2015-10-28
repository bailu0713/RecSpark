package com.ctvit.ott

import java.sql.{ResultSet, DriverManager, Connection}
import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.ctvit.{MysqlFlag, AllConfigs}
import net.sf.json.JSONObject
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkContext, SparkConf}
import redis.clients.jedis.Jedis
import scopt.OptionParser

/**
 * Created by BaiLu on 2015/10/19.
 */
object OttNewContent {

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

  /**
   * redis配置信息
   **/

  val REDIS_IP = configs.OTT_REDIS_IP
  //  val REDIS_IP2 = configs.BOX_REDIS_IP2
  val REDIS_PORT = configs.OTT_REDIS_PORT

  private case class Params(
                             recNumber: Int = 15,
                             taskId: String = null
                             )

  def main(args: Array[String]) {

    val startTime = System.nanoTime()
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val defaultParams = Params()
    val mysqlFlag = new MysqlFlag
    val parser = new OptionParser[Params]("ContentRecParams") {
      head("set NewContentRecParams")
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
        val period = ((System.nanoTime() - startTime) / 1e6).toString.split("\\.")(0)
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
    val conf = new SparkConf().setAppName("OttNewContent")
    val sc = new SparkContext(conf)
    //不针对栏目，对于所有的栏目都用一个统计值来计算
    val allRdd = new JdbcRDD(sc, initMySQL, s"select MovieID, MovieName,TypeID from ottelementinfo where MovieID >=? and MovieID <= ? ORDER BY Year  DESC limit ${params.recNumber * 2};", 1, 2000000000, 1, extractValues)
      .distinct()
      .map { tup => if (tup._3.equals("2")) (tup._1, tup._2, 1.toString)
    else (tup._1, tup._2, 0.toString)
    }
      .map(tup => (("toplevel", tup._3), (tup._1, tup._2)))
      .groupByKey()
      .foreach(tup => insertRedis(tup._2, tup._1._2, ""))


    val levelIdArr = catalogLeveId().size()
    for (i <- 0 until levelIdArr) {
      val levelId = catalogLeveId().get(i)
      val catalogRdd = new JdbcRDD(sc, initMySQL, s"select MovieID, MovieName,TypeID from ottelementinfo where MovieID >=? and MovieID <= ? and CatalogID = '$levelId'  ORDER BY Year DESC limit ${params.recNumber};", 1, 2000000000, 1, extractValues)
        .distinct()
        .map { tup => if (tup._3.equals("2")) (tup._1, tup._2, 1.toString)
      else (tup._1, tup._2, 0.toString)
      }
        .map(tup => ((levelId, tup._3), (tup._1, tup._2)))
        .groupByKey()
        .foreach(tup => insertRedis(tup._2, tup._1._2, tup._1._1))
    }
    sc.stop()
  }


  def initRedis(redisip: String, redisport: Int): Jedis = {
    val jedis = new Jedis(redisip, redisport, 100000)
    jedis
  }

  def initMySQL(): Connection = {
    Class.forName(MYSQL_DRIVER)
    DriverManager.getConnection(MYSQL_CONNECT, MYSQL_DB_USER, MYSQL_DB_PASSWD)
  }

  def catalogLeveId(): util.ArrayList[String] = {
    //@date 2015-10-10
    val init = initMySQL()
    val arr = new util.ArrayList[String]
    val sql = s"select DISTINCT(CatalogID) from ottelementinfo;"
    val rs = init.createStatement().executeQuery(sql)
    while (rs.next()) {
      arr.add(rs.getString(1))
    }
    init.close()
    arr
  }

  def extractValues(r: ResultSet) = {
    (r.getString(1), r.getString(2), r.getString(3))
  }


  def insertRedis(iterable: Iterable[(String, String)], seriestype: String, levelId: String): Unit = {

    val jedis = initRedis(REDIS_IP, REDIS_PORT)
    val pipeline = jedis.pipelined()
    val list = iterable.toList
    val map = new util.HashMap[String, String]()
    val key = if (levelId.length > 1) "Newcontentlist_3_" + levelId + "_" + seriestype
    else "Newcontentlist_3_0_" + seriestype

    val keynum = jedis.llen(key).toInt
    if (list.length >= keynum) {
      for (i <- 0 until list.length) {
        val recAssetId = ""
        val recAssetPic = ""
        val recProviderId = ""
        val rank = (i + 1).toString
        map.put("assetId", recAssetId)
        map.put("assetname", list(i)._2)
        map.put("assetpic", recAssetPic)
        map.put("movieID", list(i)._1)
        map.put("providerId", recProviderId)
        map.put("rank", rank)
        val value = JSONObject.fromObject(map).toString
        pipeline.rpush(key, value)
      }
      for (j <- 0 until keynum) {
        pipeline.lpop(key)
      }
      pipeline.sync()
    }
    jedis.disconnect()
  }

}
