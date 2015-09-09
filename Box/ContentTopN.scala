package com.ctvit.box

import java.sql.{DriverManager, Connection}
import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Calendar}

import net.sf.json.JSONObject
import org.apache.spark.{SparkContext, SparkConf}
import redis.clients.jedis.Jedis
import scopt.OptionParser

/**
 * Created by BaiLu on 2015/9/6.
 */
object ContentTopN {

  private case class Params(
                             recNumber: Int = 10,
                             timeSpan: Int = 10,
                             taskId: String = null
                             )

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
  val MYSQL_QUERY = "select catalog_info.id,catalog_info.sort_index from ire_content_relation inner join catalog_info on ire_content_relation.contentId=catalog_info.id where catalog_info.type=1;"
  val MYSQL_QUERY_LEVELIDNAME = "select contentName,contentId,level1Id,if(level1Name='',1,level1Name),level2Id,if(level2Name='',1,level2Name),level3Id,if(level3Name='',1,level3Name),level4Id,if(level4Name='',1,level4Name) from ire_content_relation;"
  /**
   * redis配置信息
   **/
  val REDIS_IP = ""
  val REDIS_PORT = 6379


  def main(args: Array[String]) {

    val defaultParams = Params()
    val mysqlFlag = new MysqlFlag
    val startTime = System.nanoTime()
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val parser = new OptionParser[Params]("BehaviorRecParams") {
      head("BehaviorRecProduct: an example Recommendation app for plain text data. ")
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
        val period = ((System.nanoTime() - startTime) / 1e9).toString
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

  private def run(params: Params) {
    val conf = new SparkConf().setAppName("BehaviorRecommendaton")
    val sc = new SparkContext(conf)

    /**
     * 读取配置文件信息，之前的参数采用配置文件的方式获取
     *
     **/
    val timespan = timeSpans(params.timeSpan)
    val HDFS_DIR = s"hdfs://:8020/data/xor/vod/{$timespan}.csv"
    //    val map = mapSingleCid(MYSQL_QUERY)
    val mapcidcount = mapSingleCidCount(MYSQL_QUERY)
    val maplevelidname = maplevelIdName(MYSQL_QUERY_LEVELIDNAME)
    val rawRdd = sc.textFile(HDFS_DIR)
    /**
     * 获取 观看时间,contentid,seriestype(0,1)
     **/
    val rdd = rawRdd.map { line => val field = line.split(","); (field(5), field(0))}
      .filter(tup => tup._1.matches("\\d+"))
      .filter(tup => tup._2.matches("\\d+"))

      /**
       * 观看时长超过30秒的就计算
       **/
      .filter(tup => tup._1.toInt > 30)

      /**
       * 对于单集电视剧将其映射为电视剧的catalogid作为MovieId
       * 生成（contentid,contentidcount(总集数),seriestype(0,1)，1(1次观看)）
       * 电影则生成（contentid,1（总集数）,seriestype(0,1)，1(1次观看)）
       **/
      .map {
      tup => if (mapcidcount.containsKey(tup._2)) ((mapcidcount.get(tup._2).split("#")(0), mapcidcount.get(tup._2).split("#")(1).toInt, 1), 1)
      else ((tup._2, 1, 0), 1)
    }
      //生成((contentid,seriestype),count)
      .reduceByKey(_ + _)

      /**
       * 返回值为(contentId,seriestype,viewcount)
       **/
      .map(tup => (tup._1._1, tup._1._3, tup._2 * 1.0 / tup._1._2))
    /**
     * 返回值为(contentId,contnenName,seriestype,viewcount,各个levelId,各个levelName)
     **/
    val finalrdd = rdd.map { tup => (tup._1, maplevelidname.get(tup._1), tup._2, tup._3)}
      .filter(tup => tup._2 != null)
      .map { tup =>
      val value = tup._2.split("#")
      val contentName = value(0)
      val level1Id = value(1)
      val level1Name = value(2)
      val level2Id = value(3)
      val level2Name = value(4)
      val level3Id = value(5)
      val level3Name = value(6)
      val level4Id = value(7)
      val level4Name = value(8)

      //        //（contentid，contentname，seriestype,viewcount,level1id,level1name,level2id,level2name,level3id,level3name,level4id,level4name）
      (tup._1, contentName, tup._3, tup._4, level1Id, level1Name, level2Id, level2Name, level3Id, level3Name, level4Id, level4Name)
    }

    /**
     * level1id插入redis
     * seriestype没有考虑
     **/
    finalrdd
      .filter(tup => tup._5 != "0")
      .groupBy(tup => (tup._5, tup._6))
      .map { tup =>
      (tup._1._1, tup._1._2, sortByViewcountTopK(tup._2, params.recNumber))
    }
      .foreach(tup => insertRedis(tup._1, tup._2, tup._3, "no", 0))

    //加入seriestype
        finalrdd
          .filter(tup => tup._5 != "0")
          .groupBy(tup => (tup._5, tup._6,tup._3))
          .map { tup =>
          (tup._1._1, tup._1._2,sortByViewcountTopK(tup._2, params.recNumber),tup._1._3)
        }
          .foreach(tup => insertRedis(tup._1, tup._2, tup._3,tup._4.toString,1))

    /**
     * level2id插入redis
     * seriestype没有考虑
     **/
    finalrdd
      .filter(tup => tup._7 != "0")
      .groupBy(tup => (tup._7, tup._8)).map { tup =>
      (tup._1._1, tup._1._2, sortByViewcountTopK(tup._2, params.recNumber))
    }
      .foreach(tup => insertRedis(tup._1, tup._2, tup._3, "no", 0))
    //seriestype=0
        finalrdd
          .filter(tup => tup._7 != "0")
          .groupBy(tup => (tup._7, tup._8,tup._3))
          .map { tup =>
          (tup._1._1, tup._1._2,sortByViewcountTopK(tup._2, params.recNumber),tup._1._3)
        }
          .foreach(tup => insertRedis(tup._1, tup._2, tup._3,tup._4.toString,1))
    /**
     * level3id插入redis
     * seriestype没有考虑
     **/
    finalrdd
      .filter(tup => tup._9 != "0")
      .groupBy(tup => (tup._9, tup._10)).map { tup =>
      (tup._1._1, tup._1._2, sortByViewcountTopK(tup._2, params.recNumber))
    }
      .foreach(tup => insertRedis(tup._1, tup._2, tup._3, "no", 0))
    //seriestype=0
        finalrdd
          .filter(tup => tup._9 != "0")
          .groupBy(tup => (tup._9, tup._10,tup._3))
          .map { tup =>
          (tup._1._1, tup._1._2,sortByViewcountTopK(tup._2, params.recNumber),tup._1._3)
        }
          .foreach(tup => insertRedis(tup._1, tup._2, tup._3,tup._4.toString,1))

    /**
     * level4id插入redis
     * seriestype没有考虑
     **/

    finalrdd
      .filter(tup => tup._11 != "0")
      .groupBy(tup => (tup._11, tup._12)).map { tup =>
      (tup._1._1, tup._1._2, sortByViewcountTopK(tup._2, params.recNumber))
    }
      .foreach(tup => insertRedis(tup._1, tup._2, tup._3, "no", 0)) //seriestype=0

    //seriestype=1
        finalrdd
          .filter(tup => tup._11 != "0")
          .groupBy(tup => (tup._11, tup._12,tup._3))
          .map { tup =>
          (tup._1._1, tup._1._2,sortByViewcountTopK(tup._2, params.recNumber),tup._1._3)
        }
          .foreach(tup => insertRedis(tup._1, tup._2, tup._3,tup._4.toString,1))
  }


  /**
   * 对于groupby后返回的iterable（contentid，contentname，seriestype,viewcount,level1id,level1name,level2id,level2name,level3id,level3name,level4id,level4name），按照viewcount排序
   **/
  def sortByViewcountTopK(iterable: Iterable[(String, String, Int, Double, String, String, String, String, String, String, String, String)], topK: Int): String = {
    val list = iterable.toList
    val sortlist = list.sortBy(_._4.toInt).reverse //升序排列返回结果
    var rec = ""
    val reclength = list.length
    var i = 0
    if (reclength >= topK) {
      while (i < topK) {
        rec += sortlist(i)._1 + "," + sortlist(i)._2 + "," + sortlist(i)._3 + "," + sortlist(i)._4 + "," + sortlist(i)._5 + "," + sortlist(i)._6 + "," + sortlist(i)._7 + "," + sortlist(i)._8 + "," + sortlist(i)._9 + "," + sortlist(i)._10 + "," + sortlist(i)._11 + "," + sortlist(i)._12 + "#"
        i += 1
      }
      rec
    }
    else {
      while (i < reclength) {
        rec += sortlist(i)._1 + "," + sortlist(i)._2 + "," + sortlist(i)._3 + "," + sortlist(i)._4 + "," + sortlist(i)._5 + "," + sortlist(i)._6 + "," + sortlist(i)._7 + "," + sortlist(i)._8 + "," + sortlist(i)._9 + "," + sortlist(i)._10 + "," + sortlist(i)._11 + "," + sortlist(i)._12 + "#"
        i += 1
      }
      rec
    }

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
    val init = initMySQL()
    val rs = init.createStatement().executeQuery(sql)
    while (rs.next()) {
      val arr = rs.getString(2).split(";")
      if (arr.length > 1) {
        for (i <- 0 until arr.length) {
          map.put(arr(i), rs.getString(1))
        }
      }
      else
        map.put(rs.getString(2), rs.getString(1))
    }
    map
  }

  /**
   * 将电视剧系列的sortindex每一项作为key，catalogid与对应的集数作为value即(catalogid,count(电视剧的集数))
   * 保证只读取一次mysql，不要每次去读取
   **/
  def mapSingleCidCount(sql: String): util.HashMap[String, String] = {
    val map = new util.HashMap[String, String]()
    val init = initMySQL()
    val rs = init.createStatement().executeQuery(sql)
    while (rs.next()) {
      val arr = rs.getString(2).split(";")
      val value = rs.getString(1) + "#" + arr.length.toString
      if (arr.length > 1) {
        for (i <- 0 until arr.length) {
          map.put(arr(i), value)
        }
      }
      else
        map.put(rs.getString(2), value)
    }
    map
  }

  /**
   * 将每一个contentid映射出他的各个level的id和levelname (contentid,contntName#level1Id#level1Name#level2Id#levele2Name...)
   * 保证只读取一次mysql，不要每次去读取
   **/
  def maplevelIdName(sql: String): util.HashMap[String, String] = {
    val map = new util.HashMap[String, String]()
    val init = initMySQL()
    val rs = init.createStatement().executeQuery(sql)
    while (rs.next()) {
      //contentid作为key
      val key = rs.getString(2)
      val value = rs.getString(1) + "#" + rs.getString(3) + "#" + rs.getString(4) + "#" + rs.getString(5) + "#" + rs.getString(6) + "#" + rs.getString(7) + "#" + rs.getString(8) + "#" + rs.getString(9) + "#" + rs.getString(10)
      map.put(key, value)
    }
    map
  }

  def catalogLevelId(): util.ArrayList[String] = {
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


  def insertRedis(levelId: String, levelName: String, recItemList: String, seriestype: String, flag: Int): Unit = {
    val jedis = initRedis(REDIS_IP, REDIS_PORT)
    val map = new util.HashMap[String, String]()
    val arr = recItemList.split("#")
    /**
     * 0表示不加入类型seriestype
     * 1表示加入seriestype分电视剧与电影
     **/
    val key =
      if (flag == 0) "Topcontentlist_5_" + levelId
      else "Topcontentlist_5_" + levelId + "_" + seriestype

    var i = 0
    val keynum = jedis.llen(key).toInt
    while (i < arr.length) {
      val recAssetId = ""
      val recAssetName = arr(i).split(",")(1)
      val recAssetPic = ""
      val recContentId = arr(i).split(",")(0)
      val recParentName = levelName
      val recParentId = levelId
      val recProviderId = ""
      val rank = arr(i).split(",")(3)
      map.put("assetId", recAssetId)
      map.put("assetname", recAssetName)
      map.put("assetpic", recAssetPic)
      map.put("movieID", recContentId)
      map.put("parentName", recParentName)
      map.put("parentid", recParentId)
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
