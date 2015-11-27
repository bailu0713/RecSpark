package com.ctvit.box

import java.sql.{DriverManager, Connection}
import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Calendar}

import com.ctvit.{MysqlFlag, AllConfigs}
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser

/**
 * Created by BaiLu on 2015/11/2.
 */
object Test {

  private case class Params(
                             recNumber: Int = 15,
                             timeSpan: Int = 30,
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
  val MYSQL_QUERY = "select catalog_info.id,catalog_info.sort_index from ire_content_relation inner join catalog_info on ire_content_relation.contentId=catalog_info.id where catalog_info.type=1;"
  val MYSQL_QUERY_LEVELIDNAME = "select contentName,contentId,level1Id,if(level1Name='',1,level1Name),level2Id,if(level2Name='',1,level2Name),level3Id,if(level3Name='',1,level3Name),level4Id,if(level4Name='',1,level4Name) from ire_content_relation;"

  def main(args: Array[String]) {

    val defaultParams = Params()
    val mysqlFlag = new MysqlFlag
    val startTime = System.nanoTime()
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val parser = new OptionParser[Params]("ContentTopNRecParams") {
      head("ChannelTopNProduct: an example Recommendation app for plain text data. ")
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

  private def run(params: Params) {
    val conf = new SparkConf().setAppName("ContentTopN")
    val sc = new SparkContext(conf)

    /**
     * 读取配置文件信息，之前的参数采用配置文件的方式获取
     *
     **/
    val timespan = timeSpans(params.timeSpan)
    val HDFS_DIR = s"hdfs://172.16.141.215:8020/data/ire/source/rec/xor/vod/{$timespan}.csv"
    //    val map = mapSingleCid(MYSQL_QUERY)
    println(HDFS_DIR)
    println(MYSQL_QUERY)
    val mapcidcount = mapSingleCidCount(MYSQL_QUERY)
    val rawRdd = sc.textFile(HDFS_DIR)
    println(rawRdd.count())

  }

  def mapSingleCidCount(sql: String): util.HashMap[String, String] = {


    //    catalog_info.id,catalog_info.sort_index
    val map = new util.HashMap[String, String]()
    try {
      val init = initMySQL()
      val rs = init.createStatement().executeQuery(sql)
      while (rs.next()) {

        //      val arr = rs.getString(2).split(";")
        if(rs.getString(2)==null)

          rs.next()

        val arr = rs.getString(2).split(";")
        println(arr)
        val value = rs.getString(1) + "#" + arr.length.toString
        println(value)
        if (arr.length > 1) {
          for (i <- 0 until arr.length) {
            map.put(arr(i), value)
          }
        }
        else
          map.put(rs.getString(2), value)
      }
    }
    catch {
      case ex: Exception =>
        println(ex)
    }
    map
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

  def initMySQL(): Connection = {
    Class.forName(MYSQL_DRIVER)
    DriverManager.getConnection(MYSQL_CONNECT, MYSQL_DB_USER, MYSQL_DB_PASSWD)
  }
}
