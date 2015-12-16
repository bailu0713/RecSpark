package com.ctvit.box

import java.sql.{DriverManager, Connection}
import java.util

import com.ctvit.AllConfigs

/**
 * Created by BaiLu on 2015/12/15.
 */
object MfdbElementID {

  val configs = new AllConfigs
  val MYSQL_HOST = configs.BOX_MYSQL_HOST
  val MYSQL_PORT = configs.BOX_MYSQL_PORT
  val MYSQL_DB = configs.BOX_MYSQL_DB
  val MYSQL_DB_USER = configs.BOX_MYSQL_DB_USER
  val MYSQL_DB_PASSWD = configs.BOX_MYSQL_DB_PASSWD

  val MYSQL_CONNECT = "jdbc:mysql://" + MYSQL_HOST + ":" + MYSQL_PORT + "/" + MYSQL_DB
  val MYSQL_DRIVER = "com.mysql.jdbc.Driver"
  val MYSQL_QUERY_ID_SORTINDEX = "select id,sort_index from catalog_info where type=1 and sort_index is not null and sort_index<>'';"
  val MYSQL_QUERYS_ID_NAME = "select id,name from element_info where genre not like '%11%' and genre not like '%20%' and genre not like '%41%';"
  val set = new util.HashSet[String]()
  val init = initMySQL()

  //MFDB电影电视剧都要
  def main(args: Array[String]) {

    everyLevel("10046284")
    val itertor = set.iterator()
    while (itertor.hasNext) {
      println(itertor.next())
    }
    init.close()
    println("集合的大小:", set.size())
  }

//  val mapcatalogid = mapCatalogId(MYSQL_QUERY_ID_SORTINDEX)
//  val mapelementid = mapElementId(MYSQL_QUERYS_ID_NAME)

  def everyLevel(id: String) = {
    val sql = s"select id,sort_index from catalog_info where parent_id=$id and sort_index is not null;"
    val rs = init.createStatement().executeQuery(sql)
    while (rs.next()) {
      val father = rs.getString(1)
      val sort_index = rs.getString(2)
      if (sort_index.length > 0)
        recursion(father, sort_index)
    }
  }

  def recursion(fatherid: String, childid: String): util.HashSet[String] = {
    val arr = childid.split(";")
    if (arr.length > 0) {
      for (i <- 0 until arr.length) {
          set.add(arr(i))
//        if (mapelementid.containsKey(arr(i))) {
//          //电影
//          set.add(arr(i))
//        }
//        if (mapcatalogid.containsKey(fatherid) ) {
//          //电视剧
//          set.add(fatherid)
//        }
        val tmpchildid = arr(i)
        val sql = s"select id,sort_index from catalog_info where id=$tmpchildid and sort_index is not null;"
        val res = init.createStatement().executeQuery(sql)
        while (res.next()) {
          val tmpfather = res.getString(1)
          val tmpsortindex = res.getString(2)
          if (tmpsortindex.length > 0) {
            recursion(tmpfather, tmpsortindex)
          }
        }
      }
    }
    set
  }

  def initMySQL(): Connection = {
    Class.forName(MYSQL_DRIVER)
    DriverManager.getConnection(MYSQL_CONNECT, MYSQL_DB_USER, MYSQL_DB_PASSWD)
  }


  def mapCatalogId(sql: String): util.HashMap[String, String] = {
    val map = new util.HashMap[String, String]()
    val rs = init.createStatement().executeQuery(sql)
    while (rs.next()) {
      val arr = rs.getString(2).split(";")
      val value = rs.getString(1)
      if (arr.length > 1) {
        for (i <- 0 until arr.length) {
          map.put(arr(i), value)
        }
      }
      else
        map.put(rs.getString(2), value)
    }
    map
  }

  def mapElementId(sql: String): util.HashMap[String, String] = {
    val map = new util.HashMap[String, String]()
    val rs = init.createStatement().executeQuery(sql)
    while (rs.next()) {
      map.put(rs.getString(1), rs.getString(2))
    }
    map
  }
}














