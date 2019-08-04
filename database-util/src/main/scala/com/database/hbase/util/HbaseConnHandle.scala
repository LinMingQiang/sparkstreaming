package com.database.hbase.util

import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.TableName
/**
 * 用于hbase的连接等操作
 */
object HbaseConnHandle {
  var conn: Connection = null
  /**
   *
   */
  def getHbaseConn(zk: String) = {
    initConn(zk)
    conn
  }
  /**
   *
   */
  def getHbaseConn(hconf: HBaseConfiguration) = {
    initConn(hconf)
    conn
  }
  /**
   *
   */
  def initConn(zk: String) {
    if (null == conn || conn.isClosed()) {
      var hconf = HBaseConfiguration.create()
      hconf.set("hbase.zookeeper.quorum", zk)
      hconf.set("hbase.zookeeper.property.clientPort", "2181")
      conn = ConnectionFactory.createConnection(hconf)
    }
  }
  /**
   *
   */
  def initConn(hconf: HBaseConfiguration) {
    if (null == conn || conn.isClosed()) {
      conn = ConnectionFactory.createConnection(hconf)
    }
  }
  /**
   *
   */
  def getTable(zk: String, name: String) = {
    initConn(zk)
    conn.getTable(TableName.valueOf(name))
  }
 /*
  * 
  */
  def getTable(conn: Connection, name: String) = {
    conn.getTable(TableName.valueOf(name))
  }

}