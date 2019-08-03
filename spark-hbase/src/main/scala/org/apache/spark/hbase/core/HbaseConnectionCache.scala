package org.apache.spark.hbase.core

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Connection
import scala.collection.mutable.HashMap
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Table
object HbaseConnectionCache {
    private val conns = new HashMap[Configuration, Connection]
    private var conf:Option[Configuration]=None
  def getHbaseConn(zookeeper: String): Connection = {
    conns.getOrElse(conf.getOrElse(getConf(zookeeper)), {
      val conn = ConnectionFactory.createConnection(conf.get)
      conns.put(conf.get,conn)
      conn
    })
  }
  def getConf(zookeeper:String)={
    var hconf = HBaseConfiguration.create()
    hconf.set("hbase.zookeeper.quorum", zookeeper)
    hconf.set("hbase.zookeeper.property.clientPort", "2181")
    conf=Some(hconf)
    hconf
  }
  /**
   * 听说是不建议缓存和池话htable
   * http://blog.csdn.net/silent1/article/details/46124229
   */
  def getTable(zookeeper: String,name:String):Table={
   getHbaseConn(zookeeper).getTable(TableName.valueOf(name))
  }
}