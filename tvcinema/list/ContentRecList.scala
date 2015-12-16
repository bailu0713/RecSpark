package com.ctvit.list

/**
 * Created by BaiLu on 2015/11/10.
 */

import java.sql.{DriverManager, ResultSet}
import java.text.SimpleDateFormat
import java.util.{Date, Random}

import com.ctvit.{AllConfigs, MysqlFlag}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis
import scopt.OptionParser

object ContentRecList {


  private case class Params(
                             recNumber: Int = 15,
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


  val NOW_YEAR = nowYear()


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

    val conf = new SparkConf().setAppName("ContentRecList")
    val sc = new SparkContext(conf)
    val non_series_data = new JdbcRDD(sc, initMySQL, "SELECT element_info.title, element_info.id, element_info.genre,element_info.create_time, element_info.country, ire_content_relation.level1Id,ire_content_relation.seriesType, ire_content_relation.level2Id, ire_content_relation.level3Id, ire_content_relation.level4Id from ire_content_relation INNER JOIN element_info ON ire_content_relation.contentId = element_info.id where element_info.id >=? and element_info.id <= ?;", 1, 2000000000, 10, extractValues)
      .filter(tup => tup._3 != "").filter(tup => tup._3 != "null")
    val series_data = new JdbcRDD(sc, initMySQL, "SELECT catalog_info.title, catalog_info.id,catalog_info.genre,catalog_info.create_time,catalog_info.country, ire_content_relation.level1Id,ire_content_relation.seriesType ,ire_content_relation.level2Id, ire_content_relation.level3Id, ire_content_relation.level4Id from ire_content_relation INNER JOIN catalog_info ON ire_content_relation.contentId = catalog_info.id where catalog_info.id >=? and catalog_info.id <= ?;", 1, 2000000000, 10, extractValues)
      .filter(tup => tup._3 != "").filter(tup => tup._3 != "null")
    val series_tv_data = new JdbcRDD(sc, initMySQL, "select ire_content_relation.contentId,catalog_info.sort_index from ire_content_relation inner join catalog_info on ire_content_relation.contentId=catalog_info.id where catalog_info.type=1  and sort_index is not null and ire_content_relation.contentId>=? and ire_content_relation.contentId<=?;", 1, 2000000000, 10, extractSeriesTvValues)
      .filter(tup => tup._2 != "").filter(tup => tup._2 != "null")

      /** 映射为tuple，电视剧映射为(catalog_id,element_id) */
      .map(tup => (tup._1, tup._2))
    val rawRdd = series_data.union(non_series_data)
    val typeRdd = rawRdd.filter(tup => tup._6 != "10001059").filter(tup => tup._7 != "1")
      .map { field => ((field._6, field._3, field._7, field._8), (field._1, field._2, parseYear(field._4), field._5))}
      .distinct()

    val typeRdd_year_country = typeRdd.filter(tup => if (tup._2._3 != "" && tup._2._4 != "") true else false).filter(tup => tup._2._3 != "无")
    val same_levele1Id_genre_series_withyearcountry = typeRdd_year_country
      .join(typeRdd_year_country)

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
      .groupByKey(15)
      //((title,contentid,level1Id,seriestype),String=title+"#"+contentid+"#"+year+"#"+country)
      .map(tup => (tup._1._2, sortByYearTopK(tup._2, params.recNumber)))



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
      .groupByKey(15)

      /**
       * 返回((title,contentid,level1Id,seriestype),reclist)
       **/
      .map(tup => (tup._1._2, sortByYearTopK(tup._2, params.recNumber)))

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
    val tvRdds = series_tv_rdd
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
      .groupByKey(15)
      //(contentid,(title,level1Id,seriestype,iterable=[(title,contentid,year,country),...]))
      .map(tup => (tup._1._2, (tup._1._1, tup._1._4, tup._1._5, tup._2)))
      .join(series_tv_data)
      //(contentid(目标要推荐的电视剧),title,level1Id,seriestype,iterable(推荐的其他的电视剧),sortindex)
      //      .map(tup => (tup._1, sortByYearTopK(tup._2._1._4, params.recNumber)))
      .map(tup => (tup._1, sortByYearTopK(tup._2._1._4, params.recNumber), tup._2._2))

    val tvRdd_catalogid = tvRdds.map(tup => (tup._1, tup._2))

    val tvRdd_elementid = tvRdds.map(tup => (tup._3, tup._2))

    val tv = tvRdd_elementid.map { tup =>
      val arr = tup._1.split(";")
      (arr, tup._2)
    }

      .map { tup =>
      var res = ""
      var i = 0
      while (i < tup._1.length) {
        res += tup._1(i) + "," + tup._2 + "@"
        i += 1
      }
      res
    }
      .flatMap(line => line.split("@"))
      .filter{tup=>tup.split(",").length==2}
      .map { tup => val field = tup.split(","); (field(0), field(1))}


    val finalrdd = same_level1Id_genre_series_withnoyearnocountry
      .union(same_levele1Id_genre_series_withyearcountry)
      .union(tvRdd_catalogid)
      .union(tv)
      .repartition(15)


    val df = new SimpleDateFormat("yyyyMMdd")
    val today = df.format(new Date())
    val HDFS_DIR_RECLIST = s"hdfs://172.16.141.215:8020/data/ire/result/rec/content/$today"
    val hadoopconf = new Configuration()
    val fs = FileSystem.get(hadoopconf)
    val path = new Path(HDFS_DIR_RECLIST)
    if (fs.exists(path)) {
      fs.delete(path, true)
      finalrdd.saveAsTextFile(HDFS_DIR_RECLIST)
    }
    else
      finalrdd.saveAsTextFile(HDFS_DIR_RECLIST)

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
    val sortlist = list.sortBy(_._3.toInt).reverse
    var rec = ""
    val reclength = list.length
    var i = 0
    if (reclength >= topK) {
      while (i < topK) {
        //@date 2015-10-09
        val j = random.nextInt(reclength)
        if (i == 0) {
          rec += sortlist(j)._2 + "#"
          i += 1
        }
        else {
          if (rec.indexOf(sortlist(i)._1.toString) < 0)
            rec += sortlist(j)._2 + "#"
          i += 1

        }
      }
      rec
    }
    else {
      while (i < reclength) {
        if (i == 0) {
          rec += sortlist(i)._2 + "#"
          i += 1
        }
        else {
          if (rec.indexOf(sortlist(i)._1.toString) < 0 && rec.indexOf(sortlist(i)._2.toString) < 0)
            rec += sortlist(i)._2 + "#"
          i += 1
        }
      }
      rec
    }
  }

}
