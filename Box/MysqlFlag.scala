package com.ctvit.box

import java.sql.{DriverManager, Connection}

/**
 * Created by BaiLu on 2015/8/21.
 */
class MysqlFlag {

  val MYSQL_HOST = ""
  val MYSQL_PORT = ""
  val MYSQL_DB = ""
  val MYSQL_DB_USER = ""
  val MYSQL_DB_PASSWD = ""
  val MYSQL_CONNECT = "jdbc:mysql://" + MYSQL_HOST + ":" + MYSQL_PORT + "/" + MYSQL_DB
  val MYSQL_DRIVER = "com.mysql.jdbc.Driver"


  val init=initMySQL()
  def initMySQL(): Connection = {
    Class.forName(MYSQL_DRIVER)
    DriverManager.getConnection(MYSQL_CONNECT, MYSQL_DB_USER, MYSQL_DB_PASSWD)
  }

  def runSuccess(taskId:String,endTime:String,period:String){
    val MYSQL_SUCCESS = s"update task_state set endTime='$endTime',endFlag='1',period='$period' where taskName='$taskId';"
    init.createStatement().execute(MYSQL_SUCCESS)
  }
  def runFail(taskId:String,errTime:String){
    val MYSQL_FAIL = s"update task_state set errTime='$errTime',errFlag='1' where taskName='$taskId';"
    init.createStatement().execute(MYSQL_FAIL)

  }


}
