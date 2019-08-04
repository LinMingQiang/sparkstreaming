package com.kafka.zk.util

import org.I0Itec.zkclient.ZkClient
import java.util.Properties
import kafka.admin.AdminUtils
import kafka.common.TopicAndPartition
import kafka.api.OffsetRequest
import kafka.api.PartitionOffsetRequestInfo
import kafka.consumer.SimpleConsumer
import scala.util.control.NonFatal
import kafka.api.PartitionOffsetsResponse
import kafka.common.ErrorMapping
import kafka.api.TopicMetadata
import kafka.api.TopicMetadataRequest
import scala.util.Random
import kafka.api.TopicMetadataResponse
import kafka.api.PartitionMetadata
import scala.collection.JavaConversions._
private[kafka]
class KafkaUtil(val kafkaParams: Map[String, String]) {
  @transient private var _config: SimpleConsumerConfig = null
  def config: SimpleConsumerConfig = this.synchronized {
    if (_config == null) {
      _config = SimpleConsumerConfig(kafkaParams)
    }
    _config
  }
  def connect(host: String, port: Int): SimpleConsumer = {
    new SimpleConsumer(host, port, 10000, 100000, "")
  }
  /**
   * 时间：2018-01-12
   * 功能：获取kakfa中的topic的meta信息
   */
  def getTopicAndPartitions(topics: Set[String]): Set[TopicAndPartition] = {
    val metas = getPartitionMetadata(topics)
    if (metas != null) {
      metas.flatMap { tm =>
        tm.partitionsMetadata.map { pm: PartitionMetadata =>
          TopicAndPartition(tm.topic, pm.partitionId)
        }
      }
    } else null
  }
  /**
   * 时间：2018-01-12
   * 功能：获取kakfa中的topic的meta信息
   */
  def getPartitionMetadata(topics: Set[String]): Set[TopicMetadata] = {
    val req = TopicMetadataRequest(TopicMetadataRequest.CurrentVersion, 0, "", topics.toSeq)
    withBrokers(config.seedBrokers) { consumer =>
      val resp: TopicMetadataResponse = consumer.send(req)
      return resp.topicsMetadata.toSet
    }
    null
  }


  /**
   * 时间：2018-01-12
   * 功能：获取kakfa中的topic的leader信息
   */
  def findLeaders(
    topicAndPartitions: Set[TopicAndPartition]
    ): Map[TopicAndPartition, (String, Int)] = {
    val topics = topicAndPartitions.map(_.topic)
    val response = getPartitionMetadata(topics)
    response.flatMap { tm: TopicMetadata =>
      tm.partitionsMetadata.flatMap { pm: PartitionMetadata =>
        val tp = TopicAndPartition(tm.topic, pm.partitionId)
        if (topicAndPartitions(tp)) {
          pm.leader.map { l =>
            tp -> (l.host -> l.port)
          }
        } else {
          None
        }
      }
    }.toMap
  }

  def flip[K, V](m: Map[K, V]): Map[V, Seq[K]] =
    m.groupBy(_._2).map { kv =>
      kv._1 -> kv._2.keys.toSeq
    }
  /**
   * 获取这些topicpartition的最新偏移量
   */
  def getLatestLeaderOffsets(
    topics: Set[String],
    zkClient: ZkClient): Map[TopicAndPartition, Long] = {
    val topicandpartition = getTopicAndPartitions(topics)//
    val topicleaders = findLeaders(topicandpartition)//
    getLeaderOffsets(topicleaders, OffsetRequest.LatestTime, 1)//
  }
  def getLeaderOffsets(
    topicAndPartitions: Map[TopicAndPartition, (String, Int)],
    before: Long,
    maxNumOffsets: Int): Map[TopicAndPartition, Long] = {
    val leaderTp: Map[(String, Int), Seq[TopicAndPartition]] = flip(topicAndPartitions)
    val leaders = leaderTp.keys
    var result = Map[TopicAndPartition, Long]()
    withBrokers(leaders) { consumer => //对每个leader做以下处理
      val partitionsToGetOffsets: Seq[TopicAndPartition] = leaderTp((consumer.host, consumer.port))
      val reqMap = partitionsToGetOffsets.map { tp: TopicAndPartition =>
        tp -> PartitionOffsetRequestInfo(before, maxNumOffsets)
      }.toMap
      val req = OffsetRequest(reqMap)
      val resp = consumer.getOffsetsBefore(req)
      val respMap = resp.partitionErrorAndOffsets
      partitionsToGetOffsets.foreach { tp: TopicAndPartition =>
        respMap.get(tp).foreach { por: PartitionOffsetsResponse =>
          if (por.error == ErrorMapping.NoError) {
            if (por.offsets.nonEmpty) {
              result += tp -> por.offsets.head
            }
          } else {
            println(por.error)
          }
        }
      }
    }
    result
  }
  def withBrokers(brokers: Iterable[(String, Int)])(fn: SimpleConsumer => Any): Unit = {
    brokers.foreach { hp =>
      var consumer: SimpleConsumer = null
      try {
        consumer = connect(hp._1, hp._2)
        fn(consumer)
      } catch {
        case NonFatal(e) => e.printStackTrace()
      } finally {
        if (consumer != null) {
          consumer.close()
        }
      }
    }
  }
}