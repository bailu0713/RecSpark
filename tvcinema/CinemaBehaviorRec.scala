package com.ctvit.tvcinema

import java.sql.{DriverManager, Connection}
import java.text.SimpleDateFormat
import java.util
import java.util.regex.Pattern
import java.util.{Calendar, Date}

import com.ctvit.{MysqlFlag, AllConfigs}
import net.sf.json.JSONObject
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.{SparkContext, SparkConf}
import redis.clients.jedis.Jedis
import scopt.OptionParser

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable
import java.util.Random

/**
 * Created by BaiLu on 2015/11/25.
 */
object CinemaBehaviorRec {

  private case class Params(
                             numIterations: Int = 10,
                             lambda: Double = 1.0,
                             rank: Int = 15,
                             numBlocks: Int = 5,
                             recNumber: Int = 20,
                             timeSpan: Int = 90,
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
  val MYSQL_QUERY = "select id,sort_index from catalog_info where parent_id ='10001719' and sort_index is not null;"


  val REDIS_IP = configs.BOX_REDIS_IP
  val REDIS_IP2 = configs.BOX_REDIS_IP2
  val REDIS_PORT = configs.BOX_REDIS_PORT


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

  def run(params: Params): Unit = {

    val conf = new SparkConf().setAppName("CinemaBehaviorRecommendaton")
    val sc = new SparkContext(conf)
    val timespan = timeSpans(params.timeSpan)
    val HDFS_DIR = s"hdfs://172.16.141.215:8020/data/ire/source/rec/vsp/vod/{$timespan}.csv"
    val rawRdd = sc.textFile(HDFS_DIR)
    val tripleRdd = rawRdd.map { line => val field = line.split(","); (field(5), field(6), field(7))}
      .filter(tup => tup._3 == "ahqhse8x8yr8ex")
      //输出（userid，contentid，1）
      .map(tup => ((tup._2, tup._1), 1))
      .reduceByKey(_ + _)

    val tripleRating = tripleRdd.map(tup => Rating(tup._1._1.toInt, tup._1._2.toInt, tup._2.toDouble))
    val model = new ALS()
      .setIterations(params.numIterations)
      .setRank(params.rank)
      .setLambda(params.lambda)
      .setBlocks(params.numBlocks)
      .run(tripleRating)
    val recRdd = model.predict(tripleRdd.map(tup => (tup._1._1.toInt, tup._1._2.toInt)))
      .map { case Rating(user, item, score) => (user, (item, score))}
      .groupByKey(15)
      .map(tup => (tup._1.toString, sortByScore(tup._2, params.recNumber)))
    recRdd.foreach(tup => insertRedis(tup._1, tup._2, params.recNumber))
    sc.stop()

  }

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

        recStr += sortList(i)._1.toString + "#"
        i += 1
      }
      recStr
    }
    else {
      while (i < listLen) {
        recStr += sortList(i)._1.toString + "#"
        i += 1
      }
      recStr
    }
  }

  def getSortindex(sql: String): mutable.HashMap[String, String] = {
    val init = initMySQL()
    val rs = init.createStatement().executeQuery(sql)
    val hashmap = new mutable.HashMap[String, String]()
    while (rs.next()) {
      val catalogid = rs.getString(1)
      val sortindex = rs.getString(2)
      hashmap.put(catalogid, sortindex)
    }
    init.close()
    hashmap
  }

  def getCatalogID(sql: String): mutable.HashMap[String, String] = {
    val init = initMySQL()
    val rs = init.createStatement().executeQuery(sql)
    val hashmap = new mutable.HashMap[String, String]()
    while (rs.next()) {
      val catalogid = rs.getString(1)
      val sortindex = rs.getString(2)
      val arr = sortindex.split(";")
      for (i <- 0 until arr.length) {
        hashmap.put(arr(i), catalogid)
      }
    }
    init.close()
    hashmap
  }

  def getCids(sql: String): mutable.ArrayBuffer[String] = {
    val init = initMySQL()
    val rs = init.createStatement().executeQuery(sql)
    val arraybuffer = new ArrayBuffer[String]()
    while (rs.next()) {
      val sortindex = rs.getString(2)
      val arr = sortindex.split(";")
      for (i <- 0 until arr.length) {
        arraybuffer.append(arr(i))
      }
    }
    init.close()
    arraybuffer
  }

  val getsortindex = getSortindex(MYSQL_QUERY)
  val getcatalogid = getCatalogID(MYSQL_QUERY)
  val contentlist = getCids(MYSQL_QUERY)

  def insertRedis(targetUser: String, recItemList: String, recNum: Int) = {
    val jedis = initRedis(REDIS_IP, REDIS_PORT)
    val jedis2 = initRedis(REDIS_IP2, REDIS_PORT)
    val pipeline = jedis.pipelined()
    val pipeline2 = jedis2.pipelined()

    val regx="^[0-9]"
    val pattern=Pattern.compile(regx)

    val map = new util.HashMap[String, String]()
    val key = targetUser + "_5_10001719_0"
    val arr = recItemList.split("#")
    val keynum = jedis.llen(key).toInt
    val keynum2 = jedis2.llen(key).toInt

    val random = new Random()

    //记录插入个数
    var count = 0
    //控制内层循环
    var j = 0
    //只用分值最高的
    val catalogid = getcatalogid.getOrElse(arr(0), "")
    //第一个电影没有下线
    if (catalogid.length > 0) {
      val recContentIds = getsortindex.get(catalogid).toString.split(";")
      while (j < recContentIds.length && count < recNum) {
        //电影没有看过
        if (recItemList.indexOf(recContentIds(j)) < 0) {
          val recAssetId = ""
          val recAssetName = ""
          val recAssetPic = ""
          val recProviderId = ""
          val rank = (count + 1).toString
          map.put("assetId", recAssetId)
          map.put("assetname", recAssetName)
          map.put("assetpic", recAssetPic)
          map.put("movieID", pattern.matcher(recContentIds(j)).replaceAll(""))
          map.put("providerId", recProviderId)
          map.put("rank", rank)
          val value = JSONObject.fromObject(map).toString
          pipeline.rpush(key, value)
          pipeline2.rpush(key, value)
          count += 1
          j += 1
        }
        //电影已经看过
        else
          j += 1
      }
      //如果还不够20个
      while (count < recNum) {
        val index = random.nextInt(contentlist.length)
        val recAssetId = ""
        val recAssetName = ""
        val recAssetPic = ""
        val recProviderId = ""
        val rank = (count + 1).toString
        map.put("assetId", recAssetId)
        map.put("assetname", recAssetName)
        map.put("assetpic", recAssetPic)
        map.put("movieID", pattern.matcher(contentlist(index)).replaceAll(""))
        map.put("providerId", recProviderId)
        map.put("rank", rank)
        val value = JSONObject.fromObject(map).toString
        pipeline.rpush(key, value)
        pipeline2.rpush(key, value)
        count += 1
      }
    }
    //第一个电影已经下线
    else {
      while (count < recNum) {
        val index = random.nextInt(contentlist.length)
        val recAssetId = ""
        val recAssetName = ""
        val recAssetPic = ""
        val recProviderId = ""
        val rank = (count + 1).toString
        map.put("assetId", recAssetId)
        map.put("assetname", recAssetName)
        map.put("assetpic", recAssetPic)
        map.put("movieID", pattern.matcher(contentlist(index)).replaceAll(""))
        map.put("providerId", recProviderId)
        map.put("rank", rank)
        val value = JSONObject.fromObject(map).toString
        pipeline.rpush(key, value)
        pipeline2.rpush(key, value)
        count += 1
      }
    }

    for (j <- 0 until keynum) {
      pipeline.lpop(key)
    }
    pipeline.sync()
    for (j <- 0 until keynum2) {
      pipeline2.lpop(key)
    }
    pipeline2.sync()
    jedis.disconnect()
    jedis2.disconnect()
  }

}
