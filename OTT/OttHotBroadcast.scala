package com.ctvit.ott

import java.sql.{DriverManager, Connection}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.ctvit.{MysqlFlag, AllConfigs}
import org.apache.spark.{SparkContext, SparkConf}
import redis.clients.jedis.Jedis
import scopt.OptionParser

/**
 * Created by BaiLu on 2015/10/19.
 */
object OttHotBroadcast {

  val configs = new AllConfigs
  val MYSQL_HOST = configs.OTT_MYSQL_HOST
  val MYSQL_PORT = configs.OTT_MYSQL_PORT
  val MYSQL_DB = configs.OTT_MYSQL_DB
  val MYSQL_DB_USER = configs.OTT_MYSQL_DB_USER
  val MYSQL_DB_PASSWD = configs.OTT_MYSQL_DB_PASSWD
  val MYSQL_CONNECT = "jdbc:mysql://" + MYSQL_HOST + ":" + MYSQL_PORT + "/" + MYSQL_DB
  val MYSQL_DRIVER = "com.mysql.jdbc.Driver"


  val REDIS_IP = configs.OTT_REDIS_IP
  val REDIS_PORT = configs.OTT_REDIS_PORT

  private case class Params(
                             recNumber: Int = 20,
                             timeSpan: Int = 30,
                             taskId: String = null
                             )

  def main(args: Array[String]) {

    val startTime = System.nanoTime()
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val defaultParams = Params()
    val mysqlFlag = new MysqlFlag
    val parser = new OptionParser[Params]("OttHotBroadCastRecParams") {
      head("set OTTChannelTopNParams")
      opt[Int]("recNumber")
        .text(s"the number of reclist default:${defaultParams.recNumber}")
        .action((x, c) => c.copy(recNumber = x))
      opt[Int]("timeSpan")
        .text(s"number of lambda, default: ${defaultParams.timeSpan}")
        .action((x, c) => c.copy(timeSpan = x))
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
      .filter { line => val field = line.split(","); field(5).equals("2") && field(4) != ""}
      //(userid,contentid)
      .map { line => val field = line.split(","); (field(7), field(3))}
      .filter(tup => tup._1.matches("\\d+"))
      .filter(tup => tup._2.matches("\\d+"))
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
    val jedis = new Jedis(redisip, redisport,100000)
    jedis
  }

  def initMySQL(): Connection = {
    Class.forName(MYSQL_DRIVER)
    DriverManager.getConnection(MYSQL_CONNECT, MYSQL_DB_USER, MYSQL_DB_PASSWD)
  }

}
