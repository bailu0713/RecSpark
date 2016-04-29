package com.ctvit.website

/**
 * Created by BaiLu on 2016/3/9.
 */

import java.sql.{DriverManager, ResultSet}
import java.text.SimpleDateFormat
import java.util.{Random, Collections, Date}
import java.util
import com.ctvit.{AllConfigs, MysqlFlag}
import net.sf.json.JSONObject
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis
import scopt.OptionParser

object WebContentRec {

  private case class Params(
                             recNumber: Int = 20,
                             taskId: String = null
                             )

  val configs = new AllConfigs
  /**
   * 测试网
   **/
  val MYSQL_HOST = configs.WEBSITE_MYSQL_HOST
  val MYSQL_PORT = configs.WEBSITE_MYSQL_PORT
  val MYSQL_DB = configs.WEBSITE_MYSQL_DB
  val MYSQL_DB_USER = configs.WEBSITE_MYSQL_DB_USER
  val MYSQL_DB_PASSWD = configs.WEBSITE_MYSQL_DB_PASSWD

  val MYSQL_CONNECT = "jdbc:mysql://" + MYSQL_HOST + ":" + MYSQL_PORT + "/" + MYSQL_DB
  val MYSQL_DRIVER = "com.mysql.jdbc.Driver"

  val REDIS_IP = configs.WEBSITE_REDIS_IP
  val REDIS_PORT = configs.WEBSITE_REDIS_PORT


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

    val conf = new SparkConf().setAppName("WebContentRec")
    val sc = new SparkContext(conf)

    val elementid = new JdbcRDD(sc, initMySQL, "SELECT MovieID,Assetfileresoucecode from etl_iae_elementinfo where movieid >=? and movieid <= ?;", 1, 2000000000, 10, extractElementValues)
    elementid
      .filter(tup => tup._1.matches("\\d+"))
      .filter(tup => tup._2.matches("\\d+"))
      .groupByKey()
      .map(tup => (tup._1, iterableToString(tup._2)))



    val catalogid = new JdbcRDD(sc, initMySQL, "SELECT Level1ID,Level2ID,MovieID from etl_iae_cataloginfo where level1ID is not null and level2ID is not null and MovieID >=? and MovieID <= ?;", 1, 2000000000, 10, extracCatalogValues)
      .filter(tup => tup._1.matches("\\d+"))
      .filter(tup => tup._2.matches("\\d+"))
      .filter(tup => tup._3.matches("\\d+"))
      .map(tup => ((tup._1, tup._2), tup._3))
    val reclist=catalogid.join(catalogid)
    .filter(tup=>tup._2._1!=tup._2._2)
      //((level1id,movieid1),movieid2)
    .map(tup=>((tup._1._1,tup._2._1),tup._2._2))
    .groupByKey()
      //(movieid1,(level1id,reclistids))
    .map(tup=>(tup._1._2,(tup._1._1,iterableToString(tup._2))))

    reclist.join(elementid)
      //movieid,sortindex,level1id,reclist
    .map(tup=>(tup._1,tup._2._2,tup._2._1._1,tup._2._1._2))
//    .foreach(println(_))
    .foreach(tup=>insertRedis(tup._1,tup._2,tup._3,tup._4))
    sc.stop()
  }

  def initRedis(redisip: String, redisport: Int): Jedis = {
    val jedis = new Jedis(redisip, redisport, 100000)
    jedis
  }

  def initMySQL() = {
    Class.forName(MYSQL_DRIVER)
    DriverManager.getConnection(MYSQL_CONNECT, MYSQL_DB_USER, MYSQL_DB_PASSWD)
  }

  def extractElementValues(r: ResultSet) = {
    (r.getString(1), r.getString(2))
  }

  def extracCatalogValues(r: ResultSet) = {
    (r.getString(1), r.getString(2), r.getString(3))
  }


  def iterableToString(iterable: Iterable[(String)]): String = {
    val str = iterable.toList.mkString(",")
    str
  }

  /**
   * 将推荐的结果写入redis
   **/
  def insertRedis(targetContentId: String, sortindex: String, level2id: String, reclist: String): Unit = {

    val jedis = initRedis(REDIS_IP, REDIS_PORT)

    val pipeline = jedis.pipelined()
    val key = targetContentId + "_7_" + level2id + "_0"
    var i = 0
    var j = 0
    val map = new util.HashMap[String, String]()
    val keynum = jedis.llen(key).toInt

    if (reclist.split(",").length > 20) {
      while (i < 20) {
        val recAssetId = ""
        val recAssetName = ""
        val recAssetPic = ""
        val recContentId = reclist.split(",")(i)
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
