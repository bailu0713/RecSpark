package com.ctvit.box

import java.sql.{ResultSet, DriverManager, Connection}
import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.ctvit.MysqlFlag
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
  val MYSQL_HOST = "172.16.168.57"
  val MYSQL_PORT = "3306"
  val MYSQL_DB = "ire"
  val MYSQL_DB_USER = "ire"
  val MYSQL_DB_PASSWD = "ZAQ!XSW@CDE#"
  val MYSQL_CONNECT = "jdbc:mysql://" + MYSQL_HOST + ":" + MYSQL_PORT + "/" + MYSQL_DB
  val MYSQL_DRIVER = "com.mysql.jdbc.Driver"

  /**
   * redis配置信息
   **/
  val REDIS_IP = "172.16.168.235"
  val REDIS_PORT = 6379

  private case class Params(
                             recNumber: Int = 30,
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
        val period = ((System.nanoTime() - startTime) / 1e6).toString
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
    //不针对栏目，对于所有的栏目都用一个统计值来计算
    val allRdd = new JdbcRDD(sc, initMySQL, s"select contentId, contentName,seriesType from ire_content_relation where contentId >=? and contentId <= ? ORDER BY resource_time DESC limit ${params.recNumber};", 1, 2000000000, 1, extractValues)
      .distinct()
      .map(tup=>(("toplevel",tup._3), (tup._1, tup._2)))
      .groupByKey()
      .foreach(tup => insertRedis(tup._2, tup._1._2,""))


    val levelIdArr = catalogLeveId().size()
    for (i <- 0 until levelIdArr) {
      val levelId = catalogLeveId().get(i)
      val catalogRdd = new JdbcRDD(sc, initMySQL, s"select contentId, contentName,seriesType from ire_content_relation where contentId >=? and contentId <= ? and (level1Id = '$levelId' or level2Id = '$levelId' or level3Id = '$levelId' or level4Id = '$levelId' or level5Id = '$levelId' or level6Id = '$levelId') ORDER BY resource_time DESC limit ${params.recNumber};", 1, 2000000000, 1, extractValues)
        .distinct()
        .map(tup => ((levelId,tup._3), (tup._1, tup._2)))
        .groupByKey()
        .foreach(tup => insertRedis(tup._2, tup._1._2,tup._1._1))
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


  def insertRedis(iterable: Iterable[(String, String)], seriestype:String,levelId: String): Unit = {
    val list = iterable.toList
    val jedis = initRedis(REDIS_IP, REDIS_PORT)
    val pipeline=jedis.pipelined()
    val map = new util.HashMap[String, String]()

    val key = if (levelId.length > 1) "Newcontentlist_5_" + levelId + "_" + seriestype
    else "Newcontentlist_5_0_" + seriestype

    val keynum = jedis.llen(key).toInt
    for (i <- 0 until list.length) {
      val recAssetId = ""
      val recAssetPic = ""
      val recProviderId = ""
      val rank = ""
      map.put("assetId", recAssetId)
      map.put("assetname", list(i)._2)
      map.put("assetpic", recAssetPic)
      map.put("movieID", list(i)._1)
      map.put("providerId", recProviderId)
      map.put("rank", rank)
      val value = JSONObject.fromObject(map).toString
      pipeline.rpush(key,value)
//      jedis.rpush(key, value)
    }

        for (j <- 0 until keynum) {
          pipeline.lpop(key)
//          jedis.lpop(key)
        }
    pipeline.sync()
    jedis.disconnect()
  }

}