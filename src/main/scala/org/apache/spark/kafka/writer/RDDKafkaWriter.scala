package org.apache.spark.kafka.writer

import org.apache.spark.rdd.RDD
import java.util.Properties
import org.apache.kafka.clients.producer.ProducerRecord

class RDDKafkaWriter[T](@transient private val rdd: RDD[T]) {

  /**
    * @author LMQ
    * @description 将rdd的数据写入kafka
    */
  def writeToKafka[K, V](producerConfig: Properties,
                         transformFunc: T => ProducerRecord[K, V]) {
    rdd.foreachPartition { partition =>
      val producer = KafkaProducerCache.getProducer[K, V](producerConfig)
      partition
        .map(transformFunc)
        .foreach(record => producer.send(record))
      producer.close()
    }
  }

  /**
    * @author LMQ
    * @description 将rdd的数据写入kafka ，并返回成功的条数
    */
  def writeToKafkaBackCount[K, V](
      producerConfig: Properties,
      transformFunc: T => ProducerRecord[K, V]): Int = {
    rdd
      .mapPartitions { partition =>
        val list = partition.toList
        val producer = KafkaProducerCache.getProducer[K, V](producerConfig)
        list
          .map(transformFunc)
          .foreach(record => producer.send(record))
        producer.close()
        Iterator.apply(list.size)
      }
      .collect()
      .toList
      .sum
  }
}
