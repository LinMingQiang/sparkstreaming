/*
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
import org.apache.kafka.common.security.JaasUtils
object ZookeeperTest {
  val zk = "solr1,solr2,mongodb3"
  def main(args: Array[String]): Unit = {
    getAlltopics
  }


  def creatTopic() {
    val topic = "my-topic";
    val partitions = 2;
    val replication = 3;
    val topicConfig = new Properties(); // add per-topic configurations settings here
    //kafka 0.8 用的AdminUtils
    AdminUtils.createTopic(getzkUtil(zk), topic, partitions, replication, topicConfig);
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
    val zkUtil = getzkUtil(zk)
    val consumer = new SimpleConsumer("kafka3", 9092, 10000, 100000,OffsetRequest.DefaultClientId);
    val topicmeta = AdminUtils.fetchTopicMetadataFromZk("mac_probelog", zkUtil)
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
   def getzkUtil(zk: String) = {
    ZkUtils.apply(zk, 30000, 30000,
                JaasUtils.isZkSecurityEnabled())
  }
}
 */
