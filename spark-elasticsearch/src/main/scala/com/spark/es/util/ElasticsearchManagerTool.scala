package com.spark.es.util

import org.elasticsearch.client.transport.TransportClient
import java.net.InetAddress

import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.TransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient
object ElasticsearchManagerTool {
  var client: TransportClient = null
  /**
    *
    * @param address
    * @param clusterName
    */
  def initClient(address: String, clusterName: String): Unit = {
    if (null == client) {
      client = new PreBuiltTransportClient(Settings.EMPTY)
      address.split(",").map(_.split(":", -1)).foreach {
        case Array(host, port) =>
          client.addTransportAddress(
            new TransportAddress(InetAddress.getByName(host), port.toInt))
        case Array(host) =>
          client.addTransportAddress(
            new TransportAddress(InetAddress.getByName(host), 9300))
      }

    }

  }
// elasticsearch 2.0
//  def initClient(address: String, clusterName: String) {
//    if (client == null) {
//      val endpoints = address.split(",").map(_.split(":", -1)).map {
//        case Array(host, port) => new InetSocketTransportAddress(InetAddress.getByName(host), port.toInt)
//        case Array(host)       => new InetSocketTransportAddress(InetAddress.getByName(host), 9300)
//      }
//      val settings = Settings.settingsBuilder().put("cluster.name", clusterName).build()
//      client = TransportClient
//        .builder()
//        .settings(settings)
//        .build()
//        .addTransportAddresses(endpoints: _*)
//    }
//  }

  /**
    *
    * @param address
    * @param clusterName
    * @return
    */
  def getESClient(address: String, clusterName: String) = {
    try {
      initClient(address, clusterName)
    } catch {
      case t: Throwable => {
        t.printStackTrace()
        client = null
        initClient(address, clusterName)
      }
    }
    client
  }

  def main(args: Array[String]): Unit = {
    val address = "192.168.30.61,192.168.30.62,192.168.30.63"
    val clusterName = "zhiziyun"
    val c = getESClient(address,clusterName)
    println(c)
  }
}
