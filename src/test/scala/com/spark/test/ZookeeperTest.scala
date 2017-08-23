package com.spark.test

import org.apache.zookeeper.ZooKeeper
import org.apache.zookeeper.Watcher
import org.apache.zookeeper.WatchedEvent
import org.I0Itec.zkclient.ZkClient
import kafka.utils.ZKStringSerializer
import kafka.utils.ZkUtils
import kafka.admin.AdminUtils
import org.I0Itec.zkclient.ZkConnection
import java.util.Properties
import org.apache.zookeeper.CreateMode

object ZookeeperTest {
  val zk = "kylin-node1,kylin-node2,kylin-node3"
  def main(args: Array[String]): Unit = {
    
  }
  //zookeepr上的没有目录文件之分
  def createFileAndDir(zkClient: ZkClient){
    //创建一个目录/文件
    val path="/test"
    val data="文件/目录的内容"
    val mode=CreateMode.PERSISTENT//短暂，持久
    zkClient.create(path, data, mode)
    
    val dataStr=zkClient.readData[String](path)
    println(dataStr)
    //可以在此"文件"下再建一个文件/目录
    zkClient.create("/test/test", data, mode)
    zkClient.getChildren("/test")//这个时候test是目录也是文件。因为他有子目录，但本身也可以存数据
  }
  
  def creatTopic() {
    val topic = "my-topic";
    val partitions = 2;
    val replication = 3;
    val topicConfig = new Properties(); // add per-topic configurations settings here
    //kafka 0.8 用的AdminUtils
    AdminUtils.createTopic(getzkClient(zk), topic, partitions, replication, topicConfig);
    //kafka 0.9.0.2.4.2.12-1 以上用的 ZkUtils
    //val (zkClient, zkConnection) = ZkUtils.createZkClientAndConnection(zkUrl, 60000, 60000)
    //val zkUtils = new ZkUtils(zkClient, zkConnection, false)
  }

  def getzkClient(zk:String) = {
    val zkClient = new ZkClient(zk, 10000, 10000, ZKStringSerializer)
    zkClient
  }
}