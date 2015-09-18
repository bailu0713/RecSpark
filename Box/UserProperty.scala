package com.ctvit.box

import java.sql.{ResultSet, DriverManager, Connection}
import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Calendar}

import com.ctvit.MysqlFlag
import net.sf.json.JSONObject
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkContext, SparkConf}
import redis.clients.jedis.Jedis
import scopt.OptionParser

/**
 * Created by BaiLu on 2015/9/11.
 */
object UserProperty {

  private case class Params(
                             recNumber: Int = 10,
                             timeSpan: Int = 30,
                             taskId: String = null
                             )

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
  val MYSQL_QUERY = "select catalog_info.id,catalog_info.sort_index from ire_content_relation inner join catalog_info on ire_content_relation.contentId=catalog_info.id where catalog_info.type=1;"
  val MYSQL_QUERY_LEVELIDNAME = "select contentName,contentId,level1Id from ire_content_relation;"
  val MYSQL_QUERY_DISTRICT = "select caid , districtindex from ire_user_district where caid>=? and caid<?;"

  /**
   * redis配置信息
   **/
  val REDIS_IP = "172.16.168.235"
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
        val period = ((System.nanoTime() - startTime) / 1e6).toString
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
    val conf = new SparkConf().setAppName("UserPropertyRecommendaton")
    val sc = new SparkContext(conf)

    val caidDistrict = new JdbcRDD(sc, initMySQL, MYSQL_QUERY_DISTRICT, 1, 2000000000, 1, extractValues)
      .distinct()

    val timespan = timeSpans(params.timeSpan)
    val HDFS_DIR = s"hdfs://172.16.141.215:8020/data/ire/source/rec/xor/vod/{$timespan}.csv"
    val mapcidcount = mapSingleCidCount(MYSQL_QUERY)
    val maplevelidname = maplevelIdName(MYSQL_QUERY_LEVELIDNAME)
    val rawRdd = sc.textFile(HDFS_DIR)
    /**
     * 获取 观看时间,contentid,seriestype(0,1)
     **/
    val rdd = rawRdd.map { line => val field = line.split(","); (field(11), field(0))}
      .filter(tup => tup._1.matches("\\d+"))
      .filter(tup => tup._2.matches("\\d+"))
      .join(caidDistrict)
      //(caid,contentid)与(caid,districtindex) 做join连接成districtindex,contentid
      .map(tup => (tup._2._2, tup._2._1))


      /**
       * 对于单集电视剧将其映射为电视剧的catalogid作为MovieId
       * 生成（contentid,contentidcount(总集数),seriestype(0,1),districtindex(地区编号)，1(1次观看)）
       * 电影则生成（contentid,1（总集数）,seriestype(0,1),districtindex(地区编号)，1(1次观看)）
       **/
      .map {
      tup => if (mapcidcount.containsKey(tup._2)) ((mapcidcount.get(tup._2).split("#")(0), mapcidcount.get(tup._2).split("#")(1).toInt, 1, tup._1), 1)
      else ((tup._2, 1, 0, tup._1), 1)
    }
      .reduceByKey(_ + _)

      /**
       * 返回值为(contentId,seriestype,viewcount,districtindex)
       **/
      .map(tup => (tup._1._1, tup._1._3, tup._2 * 1.0 / tup._1._2, tup._1._4))

      /**
       * 返回值((districtindex,seriestype,level1id),(contentid,contentname,rank(viewcount)))
       * 首先构建行向量，之后redis的key都是groupbyKey的对象
       **/
      .filter(tup => maplevelidname.containsKey(tup._1))
      .map { tup =>
      ((tup._4, tup._2, maplevelidname.get(tup._1).split("#")(1)),
        (tup._1, maplevelidname.get(tup._1).split("#")(0), tup._3))
    }
      .groupByKey()
      .foreach(tup => insertRedis(tup._1._1, tup._1._2, tup._1._3, sortByViewcountTopK(tup._2, params.recNumber)))

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
   * 将每一个contentid映射出他的各个level1id和levelname (contentid,contntName#level1Id)
   * 保证只读取一次mysql，不要每次去读取
   **/
  def maplevelIdName(sql: String): util.HashMap[String, String] = {
    val map = new util.HashMap[String, String]()
    val init = initMySQL()
    val rs = init.createStatement().executeQuery(sql)
    while (rs.next()) {
      //contentid作为key
      val key = rs.getString(2)
      val value = rs.getString(1) + "#" + rs.getString(3)
      map.put(key, value)
    }
    map
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

  def extractValues(r: ResultSet) = {
    (r.getString(1), r.getString(2))
  }

  def initMySQL(): Connection = {
    Class.forName(MYSQL_DRIVER)
    DriverManager.getConnection(MYSQL_CONNECT, MYSQL_DB_USER, MYSQL_DB_PASSWD)
  }

  def initRedis(redisip: String, redisport: Int): Jedis = {
    val jedis = new Jedis(redisip, redisport)
    jedis
  }

  /**
   * 对于groupby后返回的iterable（contentid,contentname,rank(viewcount)），按照viewcount排序
   **/
  def sortByViewcountTopK(iterable: Iterable[(String, String, Double)], topK: Int): String = {
    val list = iterable.toList
    val sortlist = list.sortBy(_._3.toInt).reverse //升序排列返回结果
    var rec = ""
    val reclength = list.length
    var i = 0
    if (reclength >= topK) {
      while (i < topK) {
        rec += sortlist(i)._1 + "," + sortlist(i)._2 + "," + sortlist(i)._3 + "#"
        i += 1
      }
      rec
    }
    else {
      while (i < reclength) {
        rec += sortlist(i)._1 + "," + sortlist(i)._2 + "," + sortlist(i)._3 + "#"
        i += 1
      }
      rec
    }

  }

  def insertRedis(districtIndex: String, seriesType: Int, level1Id: String, recItemList: String): Unit = {
    val jedis = initRedis(REDIS_IP, REDIS_PORT)
    val pipeline = jedis.pipelined()
    val map = new util.HashMap[String, String]()
    val arr = recItemList.split("#")
    val key = districtIndex + "_5_" + seriesType + "_" + level1Id
    /**
     * 0表示不加入类型seriestype
     * 1表示加入seriestype分电视剧与电影
     **/
    var i = 0
    val keynum = jedis.llen(key).toInt
    while (i < arr.length) {
      val recAssetId = ""
      val recAssetName = arr(i).split(",")(1)
      val recAssetPic = ""
      val recContentId = arr(i).split(",")(0)
      val recProviderId = ""
      val rank = arr(i).split(",")(2)
      map.put("assetId", recAssetId)
      map.put("assetname", recAssetName)
      map.put("assetpic", recAssetPic)
      map.put("movieID", recContentId)
      map.put("providerId", recProviderId)
      map.put("rank", rank)
      val value = JSONObject.fromObject(map).toString
      pipeline.rpush(key, value)
      //      jedis.rpush(key, value)
      i += 1
    }
    for (j <- 0 until keynum) {
      pipeline.lpop(key)
      //      jedis.lpop(key)
    }
    pipeline.sync()
    jedis.disconnect()
  }


}
