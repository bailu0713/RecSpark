package com.ctvit.tvcinema

import java.sql.{ResultSet, DriverManager}
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
 * Created by BaiLu on 2015/11/24.
 */
object CinemaContentRec {

  private case class Params(
                             recNumber: Int = 20,
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


  val REDIS_IP = configs.BOX_REDIS_IP
  val REDIS_IP2 = configs.BOX_REDIS_IP2
  val REDIS_PORT = configs.BOX_REDIS_PORT

  def main(args: Array[String]) {
    val startTime = System.nanoTime()
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val defaultParams = Params()
    val mysqlFlag = new MysqlFlag
    val parser = new OptionParser[Params]("TvcinemaContentRec") {
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

    val conf = new SparkConf().setAppName("ContentRecNew")
    val sc = new SparkContext(conf)
    val tvcinema_rdd = new JdbcRDD(sc, initMySQL, "select id,sort_index from catalog_info where parent_id ='10001719' and sort_index is not null and id>=? and id <= ?;", 1, 2000000000, 10, extractValues)
      .map { tup => mapEveryContentid(tup._1, tup._2)}
      .flatMap { line => val field = line.split("#")
      field
    }
      .map { tup => val field = tup.split(","); (field(0), field(1))}
    val tvcinema = tvcinema_rdd.join(tvcinema_rdd)
      .filter(tup => tup._2._1 != tup._2._2)
      .map(tup => (tup._2._1, tup._2._2))
      .groupByKey(15)
      .foreach(tup=>insertRedis(tup._1,tup._2,params.recNumber))

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

  def extractValues(r: ResultSet) = {
    (r.getString(1), r.getString(2))
  }


  def mapEveryContentid(levelid: String, contentid: String): String = {
    val sortindex = contentid.split(";")
    val size = sortindex.length
    var mapStr = ""
    var i = 0
    if (size > 0) {
      while (i < size) {
        mapStr += levelid + "," + sortindex(i) + "#"
        i += 1
      }
    }
    mapStr
  }


  def insertRedis(targetContent: String, iterable: Iterable[(String)], recnumber: Int): Unit = {
    val jedis = initRedis(REDIS_IP, REDIS_PORT)
    val jedis2 = initRedis(REDIS_IP2, REDIS_PORT)

    val pipeline = jedis.pipelined()
    val pipeline2 = jedis2.pipelined()

    val map = new util.HashMap[String, String]()
    val key = targetContent + "_5_10001719_0"
    val arr = iterable.toList
    val keynum = jedis.llen(key).toInt
    val keynum2 = jedis2.llen(key).toInt
    if (arr.length > 2) {
      var i = 0
      while (i < arr.length && i < recnumber) {
        val recAssetId = ""
        val recAssetName = ""
        val recAssetPic = ""
        val recContentId = arr(i)
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
        pipeline2.rpush(key, value)
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
