package com.ctvit.ott

/**
 * Created by BaiLu on 2015/9/18.
 */

import java.sql.{Connection, DriverManager}
import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Date}

import com.ctvit.{AllConfigs, MysqlFlag}
import net.sf.json.JSONObject
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis
import scopt.OptionParser

import scala.collection.mutable

/**
 * Created by BaiLu on 2015/8/18.
 */
object OttBehaviorRec {

  private case class Params(
                             numIterations: Int = 10,
                             lambda: Double = 1.0,
                             rank: Int = 15,
                             numBlocks: Int = 5,
                             recNumber: Int = 15,
                             timeSpan: Int = 90,
                             taskId: String = null
                             )

  //extends AbstractParams[Params]

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
  val MYSQL_CID_NAME = "select MovieName,MovieID from ottelementinfo;"

  /**
   * redis配置信息
   **/
  val REDIS_IP = configs.OTT_REDIS_IP
  val REDIS_PORT = configs.OTT_REDIS_PORT


  def main(args: Array[String]) {
    val defaultParams = Params()
    val mysqlFlag = new MysqlFlag
    val startTime = System.nanoTime()
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val parser = new OptionParser[Params]("BehaviorRecParams") {
      head("OTTBehaviorRecProduct: an example Recommendation app for plain text data. ")
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

  val cidnameMap = getCidName(MYSQL_CID_NAME)

  private def run(params: Params) {
    val conf = new SparkConf().setAppName("OttBehaviorRecommendaton")
    val sc = new SparkContext(conf)

    /**
     * 读取配置文件信息，之前的参数采用配置文件的方式获取
     *
     **/
    //    def mapSingleCid(singleCid:String)=series_tv_data.map(tup=>if(tup._2.indexOf(singleCid)>=0) tup._1.take(1) else singleCid.take(1))
    val timespan = timeSpans(params.timeSpan)
    val HDFS_DIR = s"hdfs://172.16.141.215:8020/data/ire/source/rec/ott/play/{$timespan}.csv"
    //    val map = mapSingleCid(MYSQL_QUERY)
    val rawRdd = sc.textFile(HDFS_DIR)
    val tripleRdd = rawRdd
      .filter { line => val field = line.split(","); field(5).equals("1") && field(4) != ""}
      //(userid,contentid)
      .map { line => val field = line.split(","); (field(7), field(3))}
      .filter(tup => tup._1.matches("\\d+"))
      .filter(tup => tup._2.matches("\\d+"))

      /**
       * 对于单集电视剧将其映射为电视剧的catalogid
       **/
      //      .map {
      //      tup => if (map.containsKey(tup._2)) ((tup._1, map.get(tup._2)), 1)
      //      else ((tup._1, tup._2), 1)
      //    }
      .map { tup => ((tup._1, tup._2), 1)}
      //过滤掉已经下线的contentid
      //      .filter(tup=>cidnameMap.contains(tup._1._2))
      //生成(userid,contentid,score)
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

      /**
       * 返回(targetuser，recItemList)
       **/
      .map(tup => (tup._1.toString, sortByScore(tup._2, params.recNumber)))
    recRdd.foreach(tup => insertRedis(tup._1, tup._2))

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

  /**
   * 将电视剧系列的sortindex每一项作为key，catalogid作为value
   * 保证只读取一次mysql，不要每次去读取
   **/
  def mapSingleCid(sql: String): util.HashMap[String, String] = {
    val map = new util.HashMap[String, String]()
    val init = initMySQL()
    val rs = init.createStatement().executeQuery(sql)
    while (rs.next()) {
      val arr = rs.getString(2).split(";")
      if (arr.length > 0) {
        for (i <- 0 until arr.length) {
          map.put(arr(i), rs.getString(1))
        }
      }
      else
        map.put(rs.getString(2), rs.getString(1))
    }
    map
  }

  def initMySQL(): Connection = {
    Class.forName(MYSQL_DRIVER)
    DriverManager.getConnection(MYSQL_CONNECT, MYSQL_DB_USER, MYSQL_DB_PASSWD)
  }

  /**
   * 获取内容id的名称
   **/
  def getCidName(sql: String): mutable.HashMap[String, String] = {
    val map = new mutable.HashMap[String, String]()
    val init = initMySQL()
    val rs = init.createStatement().executeQuery(sql)
    while (rs.next()) {
      map(rs.getString(2)) = rs.getString(1)
      //            map.put(rs.getString(2), rs.getString(1))
    }
    init.close()
    map
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
    val jedis = initRedis(REDIS_IP, REDIS_PORT)
    val pipeline = jedis.pipelined()

    val map = new util.HashMap[String, String]()
    val key = targetUser + "_3_0"
    val arr = recItemList.split("#")

    val keynum = jedis.llen(key).toInt
    if (arr.length >=keynum) {
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