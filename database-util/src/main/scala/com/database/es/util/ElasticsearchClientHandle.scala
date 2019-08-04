package com.database.es.util

import java.net.InetAddress
import org.elasticsearch.client.transport.TransportClient
import java.net.InetAddress
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.TransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

/**
  * @time ：2018-02-06
  * @func ：用于初始化和获取es的连接
  */
object ElasticsearchClientHandle {
  var client: TransportClient = null

  /**
    * @time :2018-02-01
    * @func :用于获取es的client
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

  /**
    * @time :2018-02-01
    * @func :用于初始化es的client
    */
  /**
    *
    * @param address
    * @param clusterName
    */
  def initClient(address: String, clusterName: String): Unit = {
    if (client == null) {
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
}
