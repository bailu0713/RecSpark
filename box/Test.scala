/**
 * Created by BaiLu on 2015/12/15.
 */
package com.ctvit.box

import java.sql.{Connection, DriverManager}
import java.text.SimpleDateFormat
import java.util
import java.util.regex.Pattern
import java.util.{Calendar, Date}

import com.ctvit.{AllConfigs, MysqlFlag}
import net.sf.json.JSONObject
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis
import scopt.OptionParser

import scala.collection.mutable

object Test {

  private case class Params(
                             numIterations: Int = 10,
                             lambda: Double = 1.0,
                             rank: Int = 15,
                             numBlocks: Int = 5,
                             recNumber: Int = 20,
                             timeSpan: Int = 90,
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
  val MYSQL_QUERY = "select id,sort_index from catalog_info where type=1 and sort_index is not null and sort_index<>''; "
  val MYSQL_CID_NAME = "select id,name from element_info where genre not like '%11%' and genre not like '%20%' and genre not like '%41%';"
  val REDIS_IP = configs.BOX_REDIS_IP
  val REDIS_IP2 = configs.BOX_REDIS_IP2
  val REDIS_PORT = configs.BOX_REDIS_PORT
  val init = initMySQL()

  def main(args: Array[String]) {
    val defaultParams = Params()
    val mysqlFlag = new MysqlFlag
    val startTime = System.nanoTime()
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val parser = new OptionParser[Params]("BehaviorRecParams") {
      head("BehaviorRecProduct: an example Recommendation app for plain text data. ")
      opt[Int]("numIterations")
        .text(s"number of iterations, default: ${defaultParams.numIterations}")
        .action((x, c) => c.copy(numIterations = x))
      opt[Double]("lambda")
        .text(s"number of lambda, default: ${defaultParams.lambda}")
        .action((x, c) => c.copy(lambda = x))
      opt[Int]("rank")
        .text(s"number of lambda, default: ${defaultParams.rank}")
        .action((x, c) => c.copy(rank = x))
      opt[Int]("numBlocks")
        .text(s"number of lambda, default: ${defaultParams.numBlocks}")
        .action((x, c) => c.copy(numBlocks = x))
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
        init.close()
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


  val mapcatalogid = mapSingleCid(MYSQL_QUERY)
  val cidnameMap = getCidName(MYSQL_CID_NAME)
  val set = new util.HashSet[String]()

  private def run(params: Params) {
    val conf = new SparkConf().setAppName("BehaviorRecommendaton")
    val sc = new SparkContext(conf)
    val timespan = timeSpans(params.timeSpan)
    //    val HDFS_DIR = s"hdfs://172.16.141.215:8020/data/ire/source/rec/xor/vod/{$timespan}.csv"
    val HDFS_DIR = s"hdfs://172.16.141.215:8020/data/ire/source/rec/vsp/vod/{$timespan}.csv"
    val rawRdd = sc.textFile(HDFS_DIR)
    mfdbAllIDS("10046284")
    println(set)
    val tripleRdd = rawRdd.map { line => val field = line.split(","); (field(6), field(5), field(7))}
      //@date 2016-04-06过滤只包含免费点播的
      .filter(tup=>tup._1.length<11 && tup._2.length<11)
      .filter(tup => tup._1.matches("\\d+"))
      .filter(tup => tup._2.matches("\\d+"))
      .filter(tup => tup._3 == "ahyuvzq53adjct")
      .filter(tup=>set.contains(tup._2))
      // 对于单集电视剧将其映射为电视剧的catalogid
      .map {
      tup => if (mapcatalogid.containsKey(tup._2)) ((tup._1, mapcatalogid.get(tup._2)), 1)
      else ((tup._1, tup._2), 1)
    }
      //生成(userid,contentid,score)
      .reduceByKey(_ + _)
    .take(100)
    .foreach(println(_))

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

  /**
   * 将电视剧系列的sortindex每一项作为key，catalogid作为value
   * 保证只读取一次mysql，不要每次去读取
   **/
  def mapSingleCid(sql: String): util.HashMap[String, String] = {
    val map = new util.HashMap[String, String]()
    val rs = init.createStatement().executeQuery(sql)
    while (rs.next()) {
      val arr = rs.getString(2).split(";")
      if (arr.length > 0) {
        for (i <- 0 until arr.length) {
          map.put(arr(i), rs.getString(1))
        }
      }
    }
    map
  }

  /**
   * 获取内容id的名称
   **/
  def getCidName(sql: String): mutable.HashMap[String, String] = {
    val map = new mutable.HashMap[String, String]()
    val rs = init.createStatement().executeQuery(sql)
    while (rs.next()) {
      map(rs.getString(1)) = rs.getString(2)
    }
    map
  }

  def mfdbAllIDS(id: String) = {
    val sql = s"select id,sort_index from catalog_info where parent_id=$id and sort_index is not null;"
    val rs = init.createStatement().executeQuery(sql)
    while (rs.next()) {
      val father = rs.getString(1)
      val sort_index = rs.getString(2)
      if (sort_index.length > 0)
        recursion(father, sort_index)
    }
  }

  def recursion(fatherid: String, childid: String): util.HashSet[String] = {
    val arr = childid.split(";")
    if (arr.length > 0) {
      for (i <- 0 until arr.length) {
        set.add(arr(i))
        val tmpchildid = arr(i)
        val sql = s"select id,sort_index from catalog_info where id=$tmpchildid and sort_index is not null;"
        val res = init.createStatement().executeQuery(sql)
        while (res.next()) {
          val tmpfather = res.getString(1)
          val tmpsortindex = res.getString(2)
          if (tmpsortindex.length > 0) {
            recursion(tmpfather, tmpsortindex)
          }
        }
      }
    }
    set
  }

  def sortByScore(iterable: Iterable[(Int, Double)], topK: Int): String = {
    /**
     * recStr返回推荐的item组合拼接成string
     **/
    val list = iterable.toList
    val sortList = list.sortBy(_._2).reverse
    val listLen = sortList.length
    var i = 0
    var recStr = ""
    if (listLen >= topK) {
      while (i < topK) {

        recStr += sortList(i)._1.toString + "," + cidnameMap.getOrElse(sortList(i)._1.toString, " ") + "#"
        i += 1
      }
      recStr
    }
    else {
      while (i < listLen) {
        recStr += sortList(i)._1.toString + "," + cidnameMap.getOrElse(sortList(i)._1.toString, " ") + "#"
        i += 1
      }
      recStr
    }
  }

  def insertRedis(targetUser: String, recItemList: String): Unit = {

    val regx = "[^0-9]"
    val pattern = Pattern.compile(regx)

    val jedis = initRedis(REDIS_IP, REDIS_PORT)
    val jedis2 = initRedis(REDIS_IP2, REDIS_PORT)

    val pipeline = jedis.pipelined()
    val pipeline2 = jedis2.pipelined()

    val map = new util.HashMap[String, String]()
    val key = targetUser + "_5_10046284_0"
    val arr = recItemList.split("#")
    val keynum = jedis.llen(key).toInt
    val keynum2 = jedis2.llen(key).toInt
    if (arr.length > 10) {
      var i = 0
      while (i < arr.length) {
        val recAssetId = ""
        val recAssetName = arr(i).split(",")(1)
        val recAssetPic = ""
        val recContentId = pattern.matcher(arr(i).split(",")(0)).replaceAll("")
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
