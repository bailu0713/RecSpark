package com.ctvit.ott

import java.sql.{ResultSet, DriverManager}
import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.ctvit.{AllConfigs, MysqlFlag}
import net.sf.json.JSONObject
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.JdbcRDD
import redis.clients.jedis.Jedis
import scopt.OptionParser

/**
 * Created by BaiLu on 2015/10/12.
 */
object OttContentRec {

  private case class Params(
                             recNumber: Int = 15,
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

  /**
   * redis配置信息
   **/

  val REDIS_IP = configs.OTT_REDIS_IP
  val REDIS_PORT = configs.OTT_REDIS_PORT


  def main(args: Array[String]) {

    val startTime = System.nanoTime()
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val defaultParams = Params()
    val mysqlFlag = new MysqlFlag
    val parser = new OptionParser[Params]("ContentRecParams") {
      head("set OTTContentRecParams")
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

    val conf = new SparkConf().setAppName("OTTContentRec")
    val sc = new SparkContext(conf)

    val rawData = new JdbcRDD(sc, initMySQL, "select MovieID,MovieName,CatalogID,TypeID,DramaTypeID from ottelementinfo where MovieID>? and MovieID<?;", 1, 2000000000, 10, extractValues)
      .filter(tup => tup._4.matches("\\d+"))
      .filter(tup => tup._5 != null && tup._5 != "NULL" && tup._5 != "" && tup._5.length >= 1)
    if (rawData.count() > 100) {
      val movie_tv = rawData
        //去掉学校的媒资数据
        .filter(tup => tup._4 != "21")
        //((TypeID,DramaTypeID),(MovieID,MovieName,CatalogID))
        .map(tup => ((tup._4, splitDaramaTypeID(tup._5)), (tup._1, tup._2, tup._3)))
      val filnal_movie_tv = movie_tv.join(movie_tv)
        .filter(tup => tup._2._1._1 != tup._2._2._1)
        //(MovieID,CatalogID),(MovieID,MovieName)
        .map(tup => ((tup._2._1._1, tup._2._1._3), (tup._2._2._1, tup._2._2._2)))
        .groupByKey()
        //没排序
        .map(tup => (tup._1._1, tup._1._2, recList(tup._2, params.recNumber)))
        .foreach(tup => insertRedis(tup._1, tup._2, tup._3))

      //数字学校的type=21，dramatype=113
      val school = rawData.filter(tup => tup._4 == "21")
        //21中TypeID，catalogID都一样，只需一个来确认
        //((TypeID,CatalogID),(MovieID,MovieName,CatalogID))
        .map(tup => ((tup._4, tup._3), (tup._1, tup._2, tup._3)))
      val finalschool = school.join(school)
        .filter(tup => tup._2._1._1 != tup._2._2._1)
        //(MovieID,CatalogID),(MovieID,Moviename)
        .map(tup => ((tup._2._1._1, tup._2._1._3), (tup._2._2._1, tup._2._2._2)))
        .groupByKey()
        .map(tup => (tup._1._1, tup._1._2, recList(tup._2, params.recNumber)))
        .foreach(tup => insertRedis(tup._1, tup._2, tup._3))

      sc.stop()
    }
  }

  def recList(iterable: Iterable[(String, String)], topK: Int): String = {
    /**
     * recStr返回推荐的item组合拼接成string
     **/
    val list = iterable.toList
    val listLen = list.length
    var i = 0
    var recStr = ""
    if (listLen >= topK) {
      while (i < topK) {

        recStr += list(i)._1.toString + "," + list(i)._2.toString + "#"
        i += 1
      }
      recStr
    }
    else {
      while (i < listLen) {
        recStr += list(i)._1.toString + "," + list(i)._2.toString + "#"
        i += 1
      }
      recStr
    }
  }

  def initRedis(redisip: String, redisport: Int): Jedis = {
    val jedis = new Jedis(redisip, redisport, 100000)
    jedis
  }

  def initMySQL() = {
    Class.forName(MYSQL_DRIVER)
    DriverManager.getConnection(MYSQL_CONNECT, MYSQL_DB_USER, MYSQL_DB_PASSWD)
  }

  def extractValues(r: ResultSet) = {
    (r.getString(1), r.getString(2), r.getString(3), r.getString(4), r.getString(5))
  }

  def splitDaramaTypeID(dramatypeid: String): String = {
    val dramatype = dramatypeid.split(",")(0)
    dramatype

  }

  def insertRedis(movieid: String, catalogid: String, recItemList: String): Unit = {
    val jedis = initRedis(REDIS_IP, REDIS_PORT)
    val pipeline = jedis.pipelined()
    val map = new util.HashMap[String, String]()
    val key = movieid + "_3_0_" + catalogid
    val arr = recItemList.split("#")
    val keynum = jedis.llen(key).toInt
    if (arr.length > keynum) {
      var i = 0
      while (i < arr.length) {
        val recAssetId = ""
        val recAssetName = arr(i).split(",")(1)
        val recAssetPic = ""
        val recContentId = arr(i).split(",")(0)
        val recProviderId = ""
        val rank = (i + 1).toString
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
