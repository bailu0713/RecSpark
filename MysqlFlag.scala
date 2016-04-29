package com.ctvit

import java.sql.{Connection, DriverManager}

/**
 * Created by BaiLu on 2015/8/21.
 */

class MysqlFlag {

  val MYSQL_FLAG_HOST = "172.16.168.236"
//  val MYSQL_FLAG_HOST = "172.28.3.79"

  val MYSQL_FLAG_PORT = "3306"
  val MYSQL_FLAG_DB = "ire"
  val MYSQL_FLAG_DB_USER = "ire"
  val MYSQL_FLAG_DB_PASSWD = "ZAQ!XSW@CDE#"
  val MYSQL_FLAG_CONNECT = "jdbc:mysql://" + MYSQL_FLAG_HOST + ":" + MYSQL_FLAG_PORT + "/" + MYSQL_FLAG_DB
  val MYSQL_DRIVER = "com.mysql.jdbc.Driver"


  val initflag=initFlagMySQL()
  def initFlagMySQL(): Connection = {
    Class.forName(MYSQL_DRIVER)
    DriverManager.getConnection(MYSQL_FLAG_CONNECT, MYSQL_FLAG_DB_USER, MYSQL_FLAG_DB_PASSWD)
  }

  def runSuccess(taskId:String,endTime:String,period:String){
    val MYSQL_SUCCESS = s"update task_state set endTime='$endTime',endFlag='1',period='$period' where taskName='$taskId';"
    initflag.createStatement().execute(MYSQL_SUCCESS)
  }
  def runFail(taskId:String,errTime:String){
    val MYSQL_FAIL = s"update task_state set errorTime='$errTime',errorFlag='1' where taskName='$taskId';"
    initflag.createStatement().execute(MYSQL_FAIL)
  }


}
