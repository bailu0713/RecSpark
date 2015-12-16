package com.ctvit.tv

import java.sql.{Connection, ResultSet, DriverManager}
import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Date}
import com.ctvit.{AllConfigs, MysqlFlag}
import net.sf.json.JSONObject
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.JdbcRDD
import redis.clients.jedis.Jedis
import scopt.OptionParser


/**
 * Created by BaiLu on 2015/8/24.
 */
object BgcTV {

  val configs=new AllConfigs
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


  val SQL_CARTOON = "select name_cn,mediaid,ind,p_pic from mediaBestv where mtype='cartoon' order by rand() limit "
  val SQL_TV = "select name_cn,mediaid,ind,p_pic from mediaBestv where mtype='tv' order by rand() limit "
  val SQL_MOVIE = "select name_cn,mediaid,ind,p_pic from mediaBestv where mtype='movie' order by rand() limit "
  val SQL_VAIETY = "select name_cn,mediaid,ind,p_pic from mediaBestv where mtype='variety' order by rand() limit "
  val SQL_SPORT = "select name_cn,mediaid,ind,p_pic from mediaBestv where mtype<>'cartoon' and mtype<>'tv' and (plots like '%运动%' or tags like '%运动%') and tags not like '%爱情%' and tags not like '%纪实%' and tags not like '%恐怖%' and tags not like '%惊悚%' order by rand() limit "
  val SQL_SCIENCE = "select name_cn,mediaid,ind,p_pic from mediaBestv where tags like '%科教%' and mtype='variety' order by rand() limit "
  val SQL_FINANCE = "select name_cn,mediaid,ind,p_pic from mediaBestv where mtype<>'cartoon' and (plots like '%证券%' or adword like '%股市%' or plots like '%股市%' or plots like '%房地产%') order by rand() limit "
  val SQL_ENTAINMENT = "select name_cn,mediaid,ind,p_pic from mediaBestv where mtype='variety' and tags not like '%纪实%' or (tags like '%纪实%' and tags like '%真人秀%' ) order by rand() limit "

  private case class Params(taskId: String = null,
                            recNumber: Int = 18)


  def main(args: Array[String]) {

    val startTime = System.nanoTime()
    val datefomat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val defaultParams = Params()
    val mysqlFlag = new MysqlFlag
    val parser = new OptionParser[Params]("ContentRecParams") {
      head("set BGCTVParams")
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
        val endTime = datefomat.format(new Date(System.currentTimeMillis()))
        val period = ((System.nanoTime() - startTime) / 1e6).toString.split("\\.")(0)
        mysqlFlag.runSuccess(params.taskId, endTime, period)
      }.getOrElse {
        parser.showUsageAsError
        sys.exit(-1)
      }
    } catch {
      case _: Exception =>
        val errTime = datefomat.format(new Date(System.currentTimeMillis()))
        parser.parse(args, defaultParams).map { params =>
          mysqlFlag.runFail(params.taskId, errTime)
        }
    }
  }

  def run(params: Params): Unit = {
    val conf = new SparkConf().setAppName("BGCTV")
    val sc = new SparkContext(conf)



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

        /**
         * 直播推荐的数据结构
         **/
        val channelMap = new util.HashMap[String, util.ArrayList[String]]()
        val channelList = new util.ArrayList[String]()
        /**
         * 点播推荐的数据结构
         **/
        val mediaMap = new util.HashMap[String, util.ArrayList[util.HashMap[String, String]]]()
        val mediaList = new util.ArrayList[util.HashMap[String, String]]()
        val mediaInnerMap = new util.HashMap[String, String]()

        /**
         * redis中数据结构
         **/
/*
        if (column.equals("")) {
          val rddnocolumn = new JdbcRDD(sc, initMySQL, s"select a.channelId from (select channelId,channelName,programTitle,serviceId from livemedia where startTime<='$dbTimeLiveLoop' and endTime>'$dbTimeLiveLoop'  and channelId <> 'cctv1' and channelId<>'cctv5'and channelId <> 'cctv6' and channelId<>'cctv3' and channelId <> 'cctv8' and channelId<>'5927c7a6dd31f38686fafa073e2e13bc' and channelId<>'590e187a8799b1890175d25ec85ea352' and channelId<>'28502a1b6bf5fbe7c6da9241db596237' and channelId<>'9291c40ec1cec1281638720c74c7245f' and channelId<>'1ce026a774dba0d13dc0cef453248fb7' and channelId<>'5dfcaefe6e7203df9fbe61ffd64ed1c4' and channelId<>'23ab87816c24f90e5865116512e12c47' and channelId<>'20831bb807a45638cfaf81df1122024d' and channelId<>'55fc65ef82e92d0e1ccb2b3f200a7529' and channelId<>'c8bf387b1824053bdb0423ef806a2227' and channelId<>'c39a7a374d888bce3912df71bcb0d580' and channelId<>'6a3f44b1abfdfb49ddd051f9e683c86d' and channelId<>'dragontv' and channelId<>'322fa7b66243b8d0edef9d761a42f263' and channelId<>'antv' and wikiTitle<>'广告')a left join (select channel,tvRating,minute_time from live_audience_rating where minute_time= '$dbTimeAudienceLoop' and date_time='$dbTimeAudienceDay')b on a.serviceId=b.channel where a.serviceId>? and a.serviceId<? order by b.tvRating desc;", 1, 2000000000, 1, extractValues)
          .distinct()
          val liveChannelIdCount = rddnocolumn.count().toInt
          rddnocolumn.take(liveChannelIdCount)

          /**
           * 直播频道超过12条
           **/
          if (liveChannelIdCount >= params.recNumber) {
            rddnocolumn.collect().foreach(channelId => channelList.add(channelId))
            //            rddnocolumn.foreach(channelId => channelList.add(channelId))
            //            channelList.add(rddnocolumn.foreach(println).toString)
            channelMap.put("rec_livelist", channelList)
            val channelobj = JSONObject.fromObject(channelMap).toString
            mediaMap.put("rec_vodlist", mediaList)
            val mediaobj = JSONObject.fromObject(mediaMap).toString
            val key = dbTimeAudienceLoop + "_all"
            val value = channelobj.concat(mediaobj).replace("}{", ",")
//            insertRedis(key, value)
          }

          /**
           * 直播频道不足推荐的个数
           **/
          else {
            rddnocolumn.collect().foreach(channelId => channelList.add(channelId))
            channelMap.put("rec_livelist", channelList)
            val channelobj = JSONObject.fromObject(channelMap).toString

            val mediaobj = rec_vod_list(SQL_VAIETY, params.recNumber - liveChannelIdCount)

            val key = dbTimeAudienceLoop + "_all"
            val value = channelobj.concat(mediaobj).replace("}{", ",")
//            insertRedis(key, value)
          }

        }
        else {
          val rddcolumn = new JdbcRDD(sc, initMySQL, s"select a.channelId from (select channelId,channelName,programTitle,serviceId from livemedia where startTime<='$dbTimeLiveLoop' and endTime>'$dbTimeLiveLoop'  and programType='$column' and channelId <> 'cctv1' and channelId<>'cctv5'and channelId <> 'cctv6' and channelId<>'cctv3' and channelId <> 'cctv8' and channelId<>'5927c7a6dd31f38686fafa073e2e13bc' and channelId<>'590e187a8799b1890175d25ec85ea352' and channelId<>'28502a1b6bf5fbe7c6da9241db596237' and channelId<>'9291c40ec1cec1281638720c74c7245f' and channelId<>'1ce026a774dba0d13dc0cef453248fb7' and channelId<>'5dfcaefe6e7203df9fbe61ffd64ed1c4' and channelId<>'23ab87816c24f90e5865116512e12c47' and channelId<>'20831bb807a45638cfaf81df1122024d' and channelId<>'55fc65ef82e92d0e1ccb2b3f200a7529' and channelId<>'c8bf387b1824053bdb0423ef806a2227' and channelId<>'c39a7a374d888bce3912df71bcb0d580' and channelId<>'6a3f44b1abfdfb49ddd051f9e683c86d' and channelId<>'dragontv' and channelId<>'322fa7b66243b8d0edef9d761a42f263' and channelId<>'antv' and wikiTitle<>'广告')a left join (select channel,tvRating,minute_time from live_audience_rating where minute_time= '$dbTimeAudienceLoop' and date_time='$dbTimeAudienceDay')b on a.serviceId=b.channel where a.serviceId>? and a.serviceId<? order by b.tvRating desc;", 1, 2000000000, 1, extractValues)
          .distinct()

          val liveChannelIdCount = rddcolumn.count().toInt
          rddcolumn.take(liveChannelIdCount)

          /**
           * 直播频道超过12条
           **/
          if (liveChannelIdCount >= params.recNumber) {
            rddcolumn.collect().foreach(channelId => channelList.add(channelId))
            channelMap.put("rec_livelist", channelList)
            val channelobj = JSONObject.fromObject(channelMap).toString
            mediaMap.put("rec_vodlist", mediaList)
            val mediaobj = JSONObject.fromObject(mediaMap).toString
            val key = dbTimeAudienceLoop + "_" + column
            val value = channelobj.concat(mediaobj).replace("}{", ",")
//            insertRedis(key, value)
          }

          /**
           * 直播频道不足推荐的个数
           **/
          else {
            rddcolumn.collect().foreach(channelId => channelList.add(channelId))
            channelMap.put("rec_livelist", channelList)
            val channelobj = JSONObject.fromObject(channelMap).toString
            val mediaobj = rec_vod_list(columnMatch(column), params.recNumber - liveChannelIdCount)
            val key = dbTimeAudienceLoop + "_" + column
            val value = channelobj.concat(mediaobj).replace("}{", ",")

//            insertRedis(key, value)
          }
        }
*/
      }
    }

    sc.stop()
  }

  def initRedis(redisip: String, redisport: Int): Jedis = {
    val jedis = new Jedis(redisip, redisport,100000)
    jedis
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

  }

  def rec_vod_list(sql: String, number: Int): String = {
    val finalsql = sql + s" $number;"
    val mediaMap = new util.HashMap[String, util.ArrayList[util.HashMap[String, String]]]()
    val mediaList = new util.ArrayList[util.HashMap[String, String]]()
    val con = initMySQL()
    val result = con.createStatement().executeQuery(finalsql)
    while (result.next()) {
      val mediaInnerMap = new util.HashMap[String, String]()
      mediaInnerMap.put("name_cn", result.getString(1))
      mediaInnerMap.put("mediaid", result.getString(2))
      mediaInnerMap.put("index", result.getString(3))
      mediaInnerMap.put("p_pic", result.getString(4))
      //list中添加的是hashmap的一个引用指向，
      // 如果将mediaInnerMap放在外面定义，list每次都引用一个对象，list会出现相同的结果
      mediaList.add(mediaInnerMap)
    }
    mediaMap.put("rec_vodlist", mediaList)
    val vodString = JSONObject.fromObject(mediaMap).toString
    con.close()
    vodString
  }

  def extractValues(r: ResultSet) = {
    r.getString(1)
  }

  /**
   * 将推荐的结果写入redis
   **/
  def insertRedis(key: String, value: String): Unit = {
    /**
     * 先将数据从list表尾处增加，再从list表头出pop出原来的数据
     * 保证list中一直有数据，不会数据丢失
     **/
    val jedis = initRedis(REDIS_IP, REDIS_PORT)
    val pipeline = jedis.pipelined()
    val keynum = jedis.llen(key).toInt

    pipeline.rpush(key, value)
    for (j <- 0 until keynum) {
      pipeline.lpop(key)
    }
    pipeline.sync()
    jedis.disconnect()
  }
}
