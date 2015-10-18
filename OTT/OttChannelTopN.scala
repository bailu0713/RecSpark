package com.ctvit.ott

import java.sql.{ResultSet, DriverManager}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.ctvit.{AllConfigs, MysqlFlag}
import net.sf.json.JSONObject
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkContext, SparkConf}
import redis.clients.jedis.Jedis
import scopt.OptionParser

/**
 * Created by BaiLu on 2015/10/13.
 */
object OttChannelTopN {
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
                             taskId: String = null
                             )

  def main(args: Array[String]) {

    val startTime = System.nanoTime()
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val defaultParams = Params()
    val mysqlFlag = new MysqlFlag
    val parser = new OptionParser[Params]("ContentRecParams") {
      head("set OTTChannelTopNParams")
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

    val conf = new SparkConf().setAppName("OttChannelTopN")
    val sc = new SparkContext(conf)
    val dfDay = new SimpleDateFormat("yyyy-MM-dd")
    val canlendar = Calendar.getInstance()
    canlendar.add(Calendar.DAY_OF_YEAR, -7)
    val dbTimeAudienceDay = dfDay.format(canlendar.getTime)

    val rawRdd = new JdbcRDD(sc, initMySQL, s"select ChannelID,ChannelName,ChannelImage from (select DVBServiceID,ChannelID,ChannelName,ChannelImage from ottlivechannel)a left join (select channel,tvRating from live_audience_rating where date_time='$dbTimeAudienceDay' and minute_time='1200')b on a.DVBServiceID=b.channel where DVBServiceID>=? and DVBServiceID<=? order by b.tvRating desc;", 1, 200000000, 1, extractValues)
    val recChannelRdd = rawRdd.collect().take(params.recNumber)
    val channelRecommend = new ChannelRecommend()
    val key = "Broadcast_1_1_1"
//    val key="Broadcast_3_1_1"

    val jedis = initRedis(REDIS_IP, REDIS_PORT)
    val pipeline = jedis.pipelined()
    val keynum = jedis.llen(key).toInt

    for (i <- 0 until recChannelRdd.length) {
      channelRecommend.setChannelId(recChannelRdd(i)._1)
      channelRecommend.setChannelname(recChannelRdd(i)._2)
      channelRecommend.setChannelpic(recChannelRdd(i)._3)
      channelRecommend.setRank((i+1).toString)
      val value = JSONObject.fromObject(channelRecommend).toString

      pipeline.rpush(key, value)
      pipeline.sync()

    }
    for (j <- 0 until keynum) {
      pipeline.lpop(key)
    }
    pipeline.sync()
    jedis.disconnect()
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
    (r.getString(1), r.getString(2), r.getString(3))
  }

  class ChannelRecommend {
    def getChannelId: String = {
      return channelId
    }

    def setChannelId(channelId: String) {
      this.channelId = channelId
    }

    def getChannelname: String = {
      return channelname
    }

    def setChannelname(channelname: String) {
      this.channelname = channelname
    }

    def getChannelpic: String = {
      return channelpic
    }

    def setChannelpic(channelpic: String) {
      this.channelpic = channelpic
    }

    def getRank: String = {
      return rank
    }

    def setRank(rank: String) {
      this.rank = rank
    }

    private var channelId: String = null
    private var channelname: String = null
    private var channelpic: String = null
    private var rank: String = null
  }


}
