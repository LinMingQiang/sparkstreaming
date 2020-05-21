package com.kafka.zk.util

import org.I0Itec.zkclient.ZkClient
import kafka.utils.ZKStringSerializer
import org.apache.zookeeper.CreateMode
import org.slf4j.LoggerFactory
import kafka.utils.ZkUtils

class ZookeeperUtil(val zk: String) {
  val PERSISTENT = CreateMode.PERSISTENT //短暂，持久
  val EPHEMERAL = CreateMode.EPHEMERAL
  var zkUtil: ZkUtils = getZkUtil(zk)
  val zkClient = zkUtil.zkClient
  lazy val LOG = LoggerFactory.getLogger("ZookeeperUtil")
  def getZkUtil(zk: String) = {
    if (zkUtil == null) {
      zkUtil = ZkUtils(zk, 10000, 10000, true)
    }
    zkUtil
  }
  def isExist(path: String) = {
    zkClient.exists(path)
  }
  def deletePath(path: String) = {
    zkClient.delete(path)
  }

  /**
    * 功能：创建目录，如果不存在就创建。
    */
  def createFileOrDir(path: String,
                      data: String = ""): Either[String, Boolean] = {
    if (!zkClient.exists(path)) {
      try {
        zkClient.create(path, data, PERSISTENT)
        new Right(true)
      } catch {
        case t: Throwable =>
          LOG.error("多级目录请使用 createMultistagePath 方法")
          LOG.error(t.toString())
          new Left(t.toString() + "\n" + t.getStackTraceString)
      }
    } else new Right(true)
  }

  /**
    * 功能：创建多级目录
    */
  def createMultistagePath(path: String) {
    val paths = path.split("/")
    var curentpath = ""
    paths.foreach { file =>
      if (!file.isEmpty()) {
        curentpath = curentpath + "/" + file
        createFileOrDir(curentpath)
      }
    }
  }

  def readData(path: String) = {
    zkClient.readData(path).toString()
  }
  def writeData(path: String, data: String) = {
    if (!zkClient.exists(path)) {
      zkClient.create(path, data, PERSISTENT)
    }
    zkClient.writeData(path, data)
  }
}
