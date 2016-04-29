package com.ctvit.tv

import java.sql.{DriverManager, Connection}
import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Calendar}

import com.ctvit.AllConfigs
import net.sf.json.JSONObject
import redis.clients.jedis.Jedis

/**
 * Created by BaiLu on 2015/12/22.
 */
object Test {
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

  val secondsPerMinute = 60
  val minutesPerHour = 60
  val hoursPerDay = 24
  val dayDuration = 7
  val msPerSecond = 1000
  val totalRecNum = 16


  val SQL_CARTOON = "select pname,pcode,episodeNum,bigimage1 from etl_aio_element_info where ptype='少儿'  order by rand() limit "
  val SQL_TV = "select pname,pcode,episodeNum,bigimage1 from etl_aio_element_info where ptype='电视剧' order by rand() limit "
  val SQL_MOVIE = "select pname,pcode,episodeNum,bigimage1 from etl_aio_element_info where type=0 and ptype='电影'  order by rand() limit "
  val SQL_VAIETY = "select pname,pcode,episodeNum,bigimage1 from etl_aio_element_info where type=0 and ptype='纪实'  and bigimage1 <>'' order by rand() limit "
  val SQL_SPORT = "select pname,pcode,episodeNum,bigimage1 from etl_aio_element_info where type=0 and ptype='体育'  and bigimage1 <>'' order by rand() limit "
  val SQL_SCIENCE = "select pname,pcode,episodeNum,bigimage1 from etl_aio_element_info where type=0 and tags like '%自然%'  and bigimage1<>'' order by rand() limit "
  val SQL_FINANCE = "select pname,pcode,episodeNum,bigimage1 from etl_aio_element_info where type=0 and ptype='财经'  and bigimage1 <>'' order by rand() limit "
  val SQL_ENTAINMENT = "select pname,pcode,episodeNum,bigimage1 from etl_aio_element_info where type=0 and ptype='娱乐'  and bigimage1 <>'' order by rand() limit "


  def main(args: Array[String]) {

    val df = new SimpleDateFormat("yyyyMMdd")
    //@date 2015-10-09
    val dfAudienceDay = new SimpleDateFormat("yyyy-MM-dd")
    val db_sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    /**
     * 时间1
     * 获取一周前的日期，转化成yyyyMMdd
     **/
    val canlendar = Calendar.getInstance()
    canlendar.add(Calendar.DAY_OF_YEAR, -7)
    val dbTimeAudienceDay = dfAudienceDay.format(canlendar.getTime)

    /**
     * 时间2
     * 获取今天的时间初始yyyy-MM-dd 00:00:00
     **/
    val todayTime = df.format(new Date())
    val dbStartLoop = df.parse(todayTime).getTime
    val startTodayTime = db_sdf.format(dbStartLoop)
    /**
     * 当天0时刻开始时间转换成秒
     **/
    val startTodayTimeSecond = db_sdf.parse(startTodayTime).getTime / msPerSecond

    for (i <- 0 until 1440) {
      /**
       * 时间3
       * 0000为开始时间
       * 设置为分钟和小时
       * 直播时间从2015-10-01 00：00：00---2015-10-01 23：59：59
       **/
      val dbTimeLiveLoop = db_sdf.format((startTodayTimeSecond + i * secondsPerMinute) * msPerSecond)
      val dbTimeAudienceLoop = dbTimeLiveLoop.split(" ")(1).replaceAll(":", "").substring(0, 4)

      for (column <- Array("", "电视剧", "电影", "少儿", "综合", "体育", "科教", "财经", "娱乐")) {
        val key = dbTimeAudienceLoop + "_" + column
        println(key)
      }

    }
  }

  def rec_vod_list(sql: String, number: Int): String = {
    val finalsql = sql + s" $number;"
    val mediaMap = new util.HashMap[String, util.ArrayList[util.HashMap[String, String]]]()
    val mediaList = new util.ArrayList[util.HashMap[String, String]]()
    val con = initMySQL()
    val result = con.createStatement().executeQuery("select pname,pcode,episodeNum,bigimage1 from etl_aio_element_info where type=0 and ptype='体育'  and bigimage1 <>'' order by rand() limit 10;")

    while (result.next()) {
      val mediaInnerMap = new util.HashMap[String, String]()
      mediaInnerMap.put("name_cn", result.getString(1))
      mediaInnerMap.put("mediaid", result.getString(2))
      mediaInnerMap.put("index", result.getString(3))
      mediaInnerMap.put("p_pic", result.getString(4))
      mediaList.add(mediaInnerMap)
    }
    mediaMap.put("rec_vodlist", mediaList)
    val vodString = JSONObject.fromObject(mediaMap).toString
    con.close()
    vodString
  }

  def initMySQL(): Connection = {
    Class.forName(MYSQL_DRIVER)
    DriverManager.getConnection(MYSQL_CONNECT, MYSQL_DB_USER, MYSQL_DB_PASSWD)
  }

  def columnMatch(column: String): String = column match {
    case "电视剧" => SQL_TV
    case "电影" => SQL_MOVIE
    case "少儿" => SQL_CARTOON
    case "综合" => SQL_VAIETY
    case "体育" => SQL_SPORT
    case "科教" => SQL_SCIENCE
    case "财经" => SQL_FINANCE
    case "娱乐" => SQL_ENTAINMENT
    case "" => SQL_TV

  }

}
