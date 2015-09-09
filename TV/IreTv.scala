package com.ctvit.tv

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.Date

import redis.clients.jedis.Jedis

/**
 * Created by BaiLu on 2015/8/24.
 */
object IreTv {
  val MYSQL_HOST = ""
  val MYSQL_PORT = ""
  val MYSQL_DB = ""
  val MYSQL_DB_USER = ""
  val MYSQL_DB_PASSWD = ""
  val MYSQL_CONNECT = "jdbc:mysql://" + MYSQL_HOST + ":" + MYSQL_PORT + "/" + MYSQL_DB
  val MYSQL_DRIVER = "com.mysql.jdbc.Driver"
  val REDIS_IP = ""
  val REDIS_PORT = 6379

  val secondsPerMinute = 60
  val minutesPerHour = 60
  val hoursPerDay = 24
  val dayDuration = 7
  val msPerSecond = 1000
  val totalRecNum = 16

  def main(args: Array[String]) {

    val df = new SimpleDateFormat("yyyyMMdd")
    val db_sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val time = df.format(new Date(System.currentTimeMillis() - secondsPerMinute * minutesPerHour * hoursPerDay * dayDuration * msPerSecond))
    //上一周时间分钟时间为00:00:00转换为当时的秒
    val dbStartLoop = df.parse(time).getTime / msPerSecond
    val dbStartTime = df.parse(time)
    val dbTime = db_sdf.format(dbStartTime)
    println(db_sdf.format(dbStartTime))
  }

  def initRedis(redisip: String, redisport: Int): Jedis = {
    val jedis = new Jedis(redisip, redisport)
    jedis
  }

  def initMySQL() = {
    Class.forName(MYSQL_DRIVER)
    DriverManager.getConnection(MYSQL_CONNECT, MYSQL_DB_USER, MYSQL_DB_PASSWD)
  }


}
