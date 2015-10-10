package com.ctvit.box

/**
 * Created by BaiLu on 2015/9/21.
 */

import java.sql.{DriverManager, ResultSet}
import java.text.SimpleDateFormat
import java.util.{Random, Collections, Date}
import java.util
import com.ctvit.MysqlFlag
import net.sf.json.JSONObject
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis
import scopt.OptionParser


/**
 * Created by BaiLu on 2015/7/24.
 */
object ContentRec {

  val MYSQL_HOST = "172.16.168.57"
  val MYSQL_PORT = "3306"
  val MYSQL_DB = ""
  val MYSQL_DB_USER = ""
  val MYSQL_DB_PASSWD = ""
  val MYSQL_CONNECT = "jdbc:mysql://" + MYSQL_HOST + ":" + MYSQL_PORT + "/" + MYSQL_DB
  val MYSQL_DRIVER = "com.mysql.jdbc.Driver"
  val REDIS_IP = ""
  val REDIS_IP2 = ""
  val REDIS_PORT = 6379
  val NOW_YEAR = nowYear()


  private case class Params(
                             recNumber: Int = 15,
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
    /**
     * 不用再在spark-submit中指定master local[*]造成申请过多资源报错，
     * 报错类型为ERROR LiveListenerBus: Listener EventLoggingListener threw an exception
     **/
    val conf = new SparkConf().setAppName("ContentRecNew")
    val sc = new SparkContext(conf)

    /** element_info  table non-series
      * (contentName,contentId,genre,year,country,level1Id(elementid),seriesType)
      * genre 0 movie/opera/e.g.  1 series TV
      */

    val non_series_data = new JdbcRDD(sc, initMySQL, "SELECT element_info.title, element_info.id, element_info.genre,element_info.create_time, element_info.country, ire_content_relation.level1Id,ire_content_relation.seriesType, ire_content_relation.level2Id, ire_content_relation.level3Id, ire_content_relation.level4Id from ire_content_relation INNER JOIN element_info ON ire_content_relation.contentId = element_info.id where element_info.id >=? and element_info.id <= ?;", 1, 2000000000, 10, extractValues)
      .filter(tup => tup._3 != "").filter(tup => tup._3 != "null")
    val series_data = new JdbcRDD(sc, initMySQL, "SELECT catalog_info.title, catalog_info.id,catalog_info.genre,catalog_info.create_time,catalog_info.country, ire_content_relation.level1Id,ire_content_relation.seriesType ,ire_content_relation.level2Id, ire_content_relation.level3Id, ire_content_relation.level4Id from ire_content_relation INNER JOIN catalog_info ON ire_content_relation.contentId = catalog_info.id where catalog_info.id >=? and catalog_info.id <= ?;", 1, 2000000000, 10, extractValues)
      .filter(tup => tup._3 != "").filter(tup => tup._3 != "null")
    val series_tv_data = new JdbcRDD(sc, initMySQL, "select ire_content_relation.contentId,catalog_info.sort_index from ire_content_relation inner join catalog_info on ire_content_relation.contentId=catalog_info.id where catalog_info.type=1 and ire_content_relation.contentId>=? and ire_content_relation.contentId<=?;", 1, 2000000000, 10, extractSeriesTvValues)
      .filter(tup => tup._2 != "").filter(tup => tup._2 != "null")

      /** 映射为tuple，电视剧映射为(catalog_id,element_id) */
      .map(tup => (tup._1, tup._2))
    val rawRdd = series_data.union(non_series_data)

    /**
     * 电影
     * 数字学校的id=10001059
     * group by 按照level1Id和genre聚合
     * ((level1Id,genre,seriesType,level2Id),(title,contentid,year,country))
     * rawRdd=16651
     * typeRdd=6932
     **/
    val typeRdd = rawRdd.filter(tup => tup._6 != "10001059").filter(tup => tup._7 != "1")
      .map { field => ((field._6, field._3, field._7, field._8), (field._1, field._2, parseYear(field._4), field._5))}
      .distinct()

    /** ####################
      * 电影
      * 针对有年份的和有国家（该部分都为电影，没有电视剧）的rdd做自join操作
      * same_levele1Id_genre_series_withyearcountry 的count=1987
      * #####################
      * */
    val typeRdd_year_country = typeRdd.filter(tup => if (tup._2._3 != "" && tup._2._4 != "") true else false).filter(tup => tup._2._3 != "无")
    val same_levele1Id_genre_series_withyearcountry = typeRdd_year_country
      .join(typeRdd_year_country)

      /**
       * 对相同的contentid过滤掉，不重复推荐
       * 会有一个问题是名字相同，但contentid不相同
       * 还需要加判断名字不相同
       **/
      .filter(tup => tup._2._1._2 != tup._2._2._2)
      .filter(tup => tup._2._1._1 != tup._2._2._1)
      .map { tup =>

      /**
       * 需要将level1Id,seriestype加入到结果集当中
       * ((title,contentid,country,level1Id,seriestype,level2Id),(title,contentid,year,country))
       **/
      ((tup._2._1._1, tup._2._1._2, tup._2._1._4, tup._1._1, tup._1._3, tup._1._4), (tup._2._2._1, tup._2._2._2, tup._2._2._3, tup._2._2._4))
    }

      /** 筛选相同国家的电影 */
      .filter(tup => tup._1._4 == tup._2._4)

      /** 筛选近五年的电影 */
      .filter(tup => tup._2._3.toInt > NOW_YEAR - 20000)
      .groupByKey()
      //((title,contentid,level1Id,seriestype),String=title+"#"+contentid+"#"+year+"#"+country)
      .map(tup => ((tup._1._1, tup._1._2, tup._1._4, tup._1._5), sortByYearTopK(tup._2, params.recNumber)))

    /** !!!!!!!!!!!
      * 插入redis
      * */
    same_levele1Id_genre_series_withyearcountry
      .foreach(tup => insertRedis(tup._1._2, tup._1._3, tup._1._4, tup._2))

    /**
     * #########################################
     * 电影
     * 针对没有年份的或者没有国家（该部分有电影，有电视剧）的rdd做自join操作
     * same_levele1Id_genre_series_withyearcountry 的count=4667
     * ##########################################
     **/
    val typeRdd_noyear_nocountry = typeRdd.filter(tup => if (tup._2._3 == "" || tup._2._4 == "" || tup._2._3 == null || tup._2._4 == null) true else false)
    val same_level1Id_genre_series_withnoyearnocountry = typeRdd_noyear_nocountry
      .join(typeRdd_noyear_nocountry)
      .filter(tup => tup._2._1._2 != tup._2._2._2)
      .filter(tup => tup._2._1._1 != tup._2._2._1)
      .map { tup =>

      /**
       * 需要将level1Id,seriestype 加入到结果集当中
       * ((title,contentid,country,level1Id,seriestype,level2Id),(title,contentid,year,country))
       **/
      ((tup._2._1._1, tup._2._1._2, tup._2._1._4, tup._1._1, tup._1._3, tup._1._4), (tup._2._2._1, tup._2._2._2, tup._2._2._3, tup._2._2._4))
    }
      .filter(tup => tup._2._3.toInt > NOW_YEAR - 20000)
      .groupByKey()

      /**
       * 返回((title,contentid,level1Id,seriestype),reclist)
       **/
      .map(tup => ((tup._1._1, tup._1._2, tup._1._4, tup._1._5), sortByYearTopK(tup._2, params.recNumber)))

    same_level1Id_genre_series_withnoyearnocountry
      .foreach(tup => insertRedis(tup._1._2, tup._1._3, tup._1._4, tup._2))
    /**
     * #########################################
     * 电视剧的推荐
     * 推荐的映射值
     * ((level1Id,genre,seriesType,level2Id),(title,contentid,year,country))
     * tvRdd.count=2019
     * ##########################################
     **/
    val series_tv_rdd = rawRdd.filter(tup => tup._6 != "10001059")
      .filter(tup => tup._7 == "1")
      .map { field => ((field._6, field._3, field._7, field._8), (field._1, field._2, parseYear(field._4), field._5))}
      .distinct()
    val tvRdd = series_tv_rdd
      .join(series_tv_rdd)

      /**
       * 对相同的contentid过滤掉，不重复推荐
       * 会有一个问题是名字相同，但contentid不相同
       * 还需要加判断名字不相同
       **/
      .filter(tup => tup._2._1._2 != tup._2._2._2)
      .filter(tup => tup._2._1._1 != tup._2._2._1)
      .map { tup =>

      /**
       * 需要将level1Id,seriestype加入到结果集当中
       * ((title,contentid,country,level1Id,seriestype),(title,contentid,year,country))
       **/
      ((tup._2._1._1, tup._2._1._2, tup._2._1._4, tup._1._1, tup._1._3), (tup._2._2._1, tup._2._2._2, tup._2._2._3, tup._2._2._4))
    }

      /** 筛选相同国家的电视剧 */
      //      .filter(tup => tup._1._4 == tup._2._4)

      /** 筛选近五年的电视剧 */
      .filter(tup => tup._2._3.toInt > NOW_YEAR - 20000)
      .groupByKey()
      //(contentid,(title,level1Id,seriestype,iterable=[(title,contentid,year,country),...]))
      .map(tup => (tup._1._2, (tup._1._1, tup._1._4, tup._1._5, tup._2)))
      .join(series_tv_data)
      //(contentid(目标要推荐的电视剧),title,level1Id,seriestype,iterable(推荐的其他的电视剧),sortindex)
      .map(tup => (tup._1, tup._2._1._1, tup._2._1._2, tup._2._1._3, sortByYearTopK(tup._2._1._4, params.recNumber), tup._2._2))

    /**
     * 将电视剧插入到redis中
     **/
    tvRdd
      .foreach(tup => insertTVRedis(tup._1, tup._3, tup._4, tup._6, tup._5))


    /**
     * #######################
     * 针对数字学校的专题推荐
     * #######################
     * group by 按照level1Id和genre聚合
     * ((level1Id,level2Id,level3Id,level4Id,seriestype),(title,contentid,year,country))
     * educationRdd=8169
     **/
    val educationRdd = rawRdd.filter(tup => tup._6 == "10001059")
      .map { field => ((field._6, field._8, field._9, field._10, field._7), (field._1, field._2, parseYear(field._4), field._5))}
      //level2Id为0的有几个值，没有太大用处
      .filter(tup => tup._1._2 != "0")
      .distinct()
    val same_levelId_education = educationRdd
      .join(educationRdd)
      //将相同的contentid去除,并将相同的名字去除
      .filter(tup => tup._2._1._2 != tup._2._2._2)
      .filter(tup => tup._2._1._1 != tup._2._2._1)
      .map { tup =>

      /**
       * 需要将level1Id,seriestype加入到结果集当中
       * ((title,contentid,country,level1Id,seriestype),(title,contentid,year,country))
       **/
      ((tup._2._1._1, tup._2._1._2, tup._2._1._4, tup._1._1, tup._1._5), (tup._2._2._1, tup._2._2._2, tup._2._2._3, tup._2._2._4))
    }
      .filter(tup => tup._2._3.toInt > NOW_YEAR - 20000)
      .groupByKey()
      .map(tup => ((tup._1._2, tup._1._4, tup._1._5), sortByYearTopK(tup._2, params.recNumber)))

    /**
     * 插入到redis
     **/
    same_levelId_education
      .foreach(tup => insertRedis(tup._1._1, tup._1._2, tup._1._3, tup._2))

    /**
     * save to hdfs
     **/
    //    val hadoopconf=new Configuration()
    //    val fs=FileSystem.get(hadoopconf)
    //    val strPath="hdfs://192.168.168.41:8020/user/bl/rdd"
    //    if (fs.exists(new Path(strPath)))
    //      fs.delete(new Path(strPath),true)
    //    same_levele1Id_series_genre.saveAsTextFile(strPath)

    sc.stop()

  }

  def initRedis(redisip: String, redisport: Int): Jedis = {
    val jedis = new Jedis(redisip, redisport)
    jedis
  }

  def initMySQL() = {
    Class.forName(MYSQL_DRIVER)
    DriverManager.getConnection(MYSQL_CONNECT, MYSQL_DB_USER, MYSQL_DB_PASSWD)
  }

  def extractValues(r: ResultSet) = {
    (r.getString(1), r.getString(2), r.getString(3), r.getString(4), r.getString(5),
      r.getString(6), r.getString(7), r.getString(8), r.getString(9), r.getString(10))
  }

  def extractSeriesTvValues(r: ResultSet) = {
    (r.getString(1), r.getString(2))
  }

  def nowYear(): Int = {
    val df = new SimpleDateFormat("yyyyMMdd")
    val nowYear = df.format(new Date())
    nowYear.toInt
  }

  def parseYear(rawYear: String): String = {
    val year = rawYear.split(" ")(0).replaceAll("-", "")
    year
  }

  /**
   * 对于groupby后返回的iterable（title,contentid,year,country），按照年份排序
   **/
  def sortByYearTopK(iterable: Iterable[(String, String, String, String)], topK: Int): String = {
    val random = new Random()
    val list = iterable.toList
    //@date 2015-10-09
    //    val sortlist =Collections.shuffle(list)
    val sortlist = list.sortBy(_._3.toInt).reverse
    var rec = ""
    val reclength = list.length
    var i = 0
    if (reclength >= topK) {
      while (i < topK) {
        //@date 2015-10-09
        val j = random.nextInt(reclength)
        if (i == 0) {
          //@date 2015-10-09
          rec += sortlist(j)._1 + "," + sortlist(j)._2 + "," + sortlist(j)._3 + "," + sortlist(j)._4 + "#"
          //          rec += sortlist(i)._1 + "," + sortlist(i)._2 + "," + sortlist(i)._3 + "," + sortlist(i)._4 + "#"
          i += 1
        }
        else {
          if (rec.indexOf(sortlist(i)._1.toString) < 0)
          //@date 2015-10-09
            rec += sortlist(j)._1 + "," + sortlist(j)._2 + "," + sortlist(j)._3 + "," + sortlist(j)._4 + "#"
          //            rec += sortlist(i)._1 + "," + sortlist(i)._2 + "," + sortlist(i)._3 + "," + sortlist(i)._4 + "#"
          i += 1

        }
      }
      rec
    }
    else {
      while (i < reclength) {
        if (i == 0) {
          rec += sortlist(i)._1 + "," + sortlist(i)._2 + "," + sortlist(i)._3 + "," + sortlist(i)._4 + "#"
          i += 1
        }
        else {
          if (rec.indexOf(sortlist(i)._1.toString) < 0 && rec.indexOf(sortlist(i)._2.toString) < 0)
            rec += sortlist(i)._1 + "," + sortlist(i)._2 + "," + sortlist(i)._3 + "," + sortlist(i)._4 + "#"
          i += 1
        }
      }
      rec
    }

  }


  /**
   * 将推荐的结果写入redis
   **/
  def insertRedis(targetContentId: String, targetlevel1Id: String, targetSeriesType: String, reclist: String): Unit = {
    /**
     * 先将数据从list表尾处增加，再从list表头出pop出原来的数据
     * 保证list中一直有数据，不会数据丢失
     **/
    val jedis = initRedis(REDIS_IP, REDIS_PORT)
    val jedis2 = initRedis(REDIS_IP2, REDIS_PORT)

    val pipeline = jedis.pipelined()
    val pipeline2 = jedis2.pipelined()
    //@date 2015-10-08
    val key = targetContentId + "_5_" + targetlevel1Id + "_0"
    //    val key = targetContentId + "_5_" + targetlevel1Id + "_" + targetSeriesType
    var i = 0
    var j = 0
    val map = new util.HashMap[String, String]()

    val keynum = jedis.llen(key).toInt
    val keynum2 = jedis2.llen(key).toInt

    while (i < reclist.split("#").length) {
      val recAssetId = ""
      val recAssetName = reclist.split("#")(i).split(",")(0)
      val recAssetPic = ""
      val recContentId = reclist.split("#")(i).split(",")(1)
      val recProviderId = ""
      val rank = ""
      map.put("assetId", recAssetId)
      map.put("assetname", recAssetName)
      map.put("assetpic", recAssetPic)
      map.put("movieID", recContentId)
      map.put("providerId", recProviderId)
      map.put("rank", rank)
      val value = JSONObject.fromObject(map).toString
      pipeline.rpush(key, value)
      pipeline2.rpush(key, value)
      //      jedis.rpush(key, value)
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
    jedis.disconnect()
    jedis2.disconnect()
  }

  def insertTVRedis(targetContentId: String, targetlevel1Id: String, targetSeriesType: String, sortIndex: String, reclist: String): Unit = {
    /**
     * 先将数据从list表尾处增加，再从list表头出pop出原来的数据
     * 保证list中一直有数据，不会数据丢失
     **/
    val jedis = initRedis(REDIS_IP, REDIS_PORT)
    val jedis2 = initRedis(REDIS_IP2, REDIS_PORT)

    val pipeline = jedis.pipelined()
    val pipeline2 = jedis2.pipelined()

    val map = new util.HashMap[String, String]()

    /**
     * 插入电视剧的catalogid
     **/
    //@date 2015-10-08
    val keys = targetContentId + "_5_" + targetlevel1Id + "_0"
    //    val keys = targetContentId + "_5_" + targetlevel1Id + "_" + targetSeriesType

    val keynums = jedis.llen(keys).toInt
    val keynums2 = jedis2.llen(keys).toInt

    for (i <- 0 until reclist.split("#").length) {
      val recAssetId = ""
      val recAssetName = reclist.split("#")(i).split(",")(0)
      val recAssetPic = ""
      val recContentId = reclist.split("#")(i).split(",")(1)
      val recProviderId = ""
      val rank = ""
      map.put("assetId", recAssetId)
      map.put("assetname", recAssetName)
      map.put("assetpic", recAssetPic)
      map.put("movieID", recContentId)
      map.put("providerId", recProviderId)
      map.put("rank", rank)
      val value = JSONObject.fromObject(map).toString
      pipeline.rpush(keys, value)
      pipeline2.rpush(keys, value)
    }

    for (j <- 0 until keynums) {
      pipeline.lpop(keys)
    }
    pipeline.sync()
    for (j <- 0 until keynums2) {
      pipeline2.lpop(keys)
    }
    pipeline2.sync()

    val targetTvArr = sortIndex.split(";")
    for (k <- 0 until targetTvArr.length) {
      /**
       * 对于多集电视剧只推每一集的ContentId，没有推整集的一个catalogid，所以没有用到参数中的targetContentId
       **/
      val targetContent = targetTvArr(k)
      //@date 2015-10-08
      val key = targetContent + "_5_" + targetlevel1Id + "_0"
      //      val key = targetContent + "_5_" + targetlevel1Id + "_" + targetSeriesType

      val keynum = jedis.llen(key).toInt
      val keynum2 = jedis2.llen(key).toInt

      for (i <- 0 until reclist.split("#").length) {
        val recAssetId = ""
        val recAssetName = reclist.split("#")(i).split(",")(0)
        val recAssetPic = ""
        val recContentId = reclist.split("#")(i).split(",")(1)
        val recProviderId = ""
        val rank = ""
        map.put("assetId", recAssetId)
        map.put("assetname", recAssetName)
        map.put("assetpic", recAssetPic)
        map.put("movieID", recContentId)
        map.put("providerId", recProviderId)
        map.put("rank", rank)
        val value = JSONObject.fromObject(map).toString

        pipeline.rpush(key, value)
        pipeline2.rpush(key, value)
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
