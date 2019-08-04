package com.database.mysql.util

import java.sql.Connection
import java.sql.DriverManager

object MysqlConnHandle {
  var conn: Connection = null
  /*
   * 
   */
  def getMysqlConn(
      jdbc:String,
      user:String,
      pass:String)={
    initMysql(jdbc, user, pass)
    conn
  }
  /*
  * 
  */
  def initMysql(
      jdbc:String,
      user:String,
      pass:String){
     if(null == conn || conn.isClosed){
      Class.forName("com.mysql.jdbc.Driver")
      conn = DriverManager.getConnection(jdbc, user, pass)
    }
  }
}