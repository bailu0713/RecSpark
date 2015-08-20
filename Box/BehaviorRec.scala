package com.ctvit.box.behavior

import java.io.FileInputStream
import java.sql.{Connection, DriverManager}
import java.text.SimpleDateFormat
import java.util
import java.util.{Properties, Calendar}
import net.sf.json.JSONObject
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.{SparkContext, SparkConf}
import redis.clients.jedis.Jedis
import scopt.OptionParser

/**
 * Created by BaiLu on 2015/8/18.
 */
object BehaviorRec {

  private case class Params(
//                     input: Seq[String] = Seq.empty,
                     numIterations: Int = 10,
                     lambda: Double = 1.0,
                     rank: Int = 15,
                     numBlocks: Int = 5,
                     recNumber: Int = 10,
                     timeSpan: Int = 90
                     ) //extends AbstractParams[Params]

  /**
   * 读取数据文件的时间间隔
   **/
  val TIME_SPAN = 90
  /**
   * ALS算法参数设置
   **/
  val ALS_ITERATION = 10
  val ALS_RANK = 15
  val ALS_LAMBDA = 0.1
  val ALS_BLOCK = 5
  /**
   * mysql配置信息
   **/
  val MYSQL_HOST = ""
  val MYSQL_PORT = "3306"
  val MYSQL_DB = ""
  val MYSQL_DB_USER = ""
  val MYSQL_DB_PASSWD = ""
  val MYSQL_CONNECT = "jdbc:mysql://" + MYSQL_HOST + ":" + MYSQL_PORT + "/" + MYSQL_DB
  val MYSQL_DRIVER = "com.mysql.jdbc.Driver"
  val MYSQL_QUERY = "select catalog.id,catalog.sort_index from relation inner join catalog on relation.contentId=catalog.id where catalog.type=1;"
  /**
   * redis配置信息
   **/
  val REDIS_IP = ""
  val REDIS_PORT = 6379
  /**
   * 推荐数量设置
   **/
  val REC_NUMBER = 10


  def main(args: Array[String]) {
    val defaultParams = Params()

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
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    }.getOrElse {
      parser.showUsageAsError
      sys.exit(1)
    }
  }

  private def run(params: Params) {
    val conf = new SparkConf().setAppName("BehaviorRecommendaton")
    val sc = new SparkContext(conf)

    /**
     * 读取配置文件信息，之前的参数采用配置文件的方式获取
     *
     **/
    //    def mapSingleCid(singleCid:String)=series_tv_data.map(tup=>if(tup._2.indexOf(singleCid)>=0) tup._1.take(1) else singleCid.take(1))
    val timespan = timeSpans(params.timeSpan)
    val HDFS_DIR = s"hdfs://ip:8020/data/ire/source/rec/xor/vod/{$timespan}.csv"
    val map = mapSingleCid(MYSQL_QUERY)
    val rawRdd = sc.textFile(HDFS_DIR)
    val tripleRdd = rawRdd.map { line => val field = line.split(","); (field(11), field(0))}
      .filter(tup => tup._1.matches("\\d+"))
      .filter(tup => tup._2.matches("\\d+"))
      /**
       * 对于单集电视剧将其映射为电视剧的catalogid
       **/
      .map {
      tup => if (map.containsKey(tup._2)) ((tup._1, map.get(tup._2)), 1)
      else ((tup._1, tup._2), 1)
    }
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
      .groupByKey(10)

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
    val jedis = new Jedis(redisip, redisport)
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
        recStr += sortList(i)._1.toString + ","
        i += 1
      }
      recStr
    }
    else {
      while (i < listLen) {
        recStr += sortList(i)._1.toString + ","
        i += 1
      }
      recStr
    }

  }

  def insertRedis(targetUser: String, recItemList: String): Unit = {
    val jedis = initRedis(REDIS_IP, REDIS_PORT)
    val map = new util.HashMap[String, String]()
    val key = targetUser + "_5_0"
    val arr = recItemList.split(",")
    var i = 0
    val keynum = jedis.llen(key).toInt
    while (i < arr.length) {
      val recAssetId = ""
      val recAssetName = ""
      val recAssetPic = ""
      val recContentId = arr(i)
      val recProviderId = ""
      val rank = ""
      map.put("assetId", recAssetId)
      map.put("assetname", recAssetName)
      map.put("assetpic", recAssetPic)
      map.put("movieID", recContentId)
      map.put("providerId", recProviderId)
      map.put("rank", rank)
      val value = JSONObject.fromObject(map).toString
      jedis.rpush(key, value)
      i += 1
    }
    for (j <- 0 until keynum) {
      jedis.lpop(key)
    }
    jedis.disconnect()
  }
}
