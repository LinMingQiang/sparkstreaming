package com.spark.es.util

import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.transport.InetSocketTransportAddress
import java.net.InetAddress
import org.elasticsearch.common.settings.Settings
import scala.collection.JavaConversions._
import java.util.Map
object ElasticsearchManagerTool {
  var client: TransportClient = null
  def initClient(address: String, clusterName: String) {
    if (client == null) {
      val endpoints = address.split(",").map(_.split(":", -1)).map {
        case Array(host, port) => new InetSocketTransportAddress(InetAddress.getByName(host), port.toInt)
        case Array(host)       => new InetSocketTransportAddress(InetAddress.getByName(host), 9300)
      }
      val settings = Settings.settingsBuilder().put("cluster.name", clusterName).build()
      client = TransportClient
        .builder()
        .settings(settings)
        .build()
        .addTransportAddresses(endpoints: _*)
    }
  }
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
}