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
import org.apache.spark.streaming.kafka.KafkaCluster
import kafka.consumer.Consumer
import kafka.consumer.ConsumerConfig
import java.util.HashMap
import kafka.common.TopicAndPartition
import kafka.api.PartitionOffsetRequestInfo
import scala.collection.JavaConversions._
import kafka.api.OffsetRequest
import kafka.consumer.SimpleConsumer
object ZookeeperTest {
  val zk = "solr1,solr2,mongodb3"
  def main(args: Array[String]): Unit = {
    getAlltopics
  }
  //zookeepr上的没有目录文件之分
  def createFileAndDir(zkClient: ZkClient) {
    //创建一个目录/文件
    val path = "/test"
    val data = "文件/目录的内容"
    val mode = CreateMode.PERSISTENT //短暂，持久
    zkClient.create(path, data, mode)

    val dataStr = zkClient.readData[String](path)
    println(dataStr)
    //可以在此"文件"下再建一个文件/目录
    zkClient.create("/test/test", data, mode)
    zkClient.getChildren("/test") //这个时候test是目录也是文件。因为他有子目录，但本身也可以存数据
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
  def getAlltopics() = {
    val props = new Properties();
    props.put("zookeeper.connect", "solr1:2181,solr2:2181,mongodb3:2181");
    props.put("group.id", "group1");
    props.put("zookeeper.session.timeout.ms", "400");
    props.put("zookeeper.sync.time.ms", "200");
    props.put("auto.commit.interval.ms", "1000");
    val zkClient = getzkClient(zk)
    val consumer = new SimpleConsumer("kafka3", 9092, 10000, 100000,OffsetRequest.DefaultClientId);
    val topicmeta = AdminUtils.fetchTopicMetadataFromZk("mac_probelog", zkClient)
    val topicAndPartitions=topicmeta.partitionsMetadata.map { partmeta => 
     new TopicAndPartition(topicmeta.topic, partmeta.partitionId);
    }
    topicmeta.partitionsMetadata.foreach { partmeta =>
      val endpoint = partmeta.leader.get
      println(endpoint.host, endpoint.port)
      val tp = new TopicAndPartition(topicmeta.topic, partmeta.partitionId);
      
      
      val requestInfo = new HashMap[TopicAndPartition, PartitionOffsetRequestInfo]();
      requestInfo.put(tp, new PartitionOffsetRequestInfo(-1, 1));//这个是固定的
      
      
      val request =OffsetRequest(requestInfo.toMap);//
      val respMap = consumer.getOffsetsBefore(request)
      consumer.close()
      respMap.partitionErrorAndOffsets.toMap.foreach{x=>
        println(x._2.offsets)
      }
    }
    //val topicmeta = AdminUtils.fetchTopicMetadataFromZk("test", zkClient)
    //topicmeta.partitionsMetadata.foreach { partmeta =>}
  }
  def getzkClient(zk: String) = {
    val zkClient = new ZkClient(zk, 10000, 10000, ZKStringSerializer)
    zkClient
  }
}