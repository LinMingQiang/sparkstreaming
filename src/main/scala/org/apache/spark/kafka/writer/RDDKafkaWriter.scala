package org.apache.spark.kafka.writer

import org.apache.spark.rdd.RDD
import java.util.Properties
import org.apache.kafka.clients.producer.ProducerRecord
private[spark]
class rddKafkaWriter[T](@transient private val rdd: RDD[T]) {
  def writeToKafka[K, V](
    producerConfig: Properties,
    transformFunc: T => ProducerRecord[K, V]) {
    rdd.foreachPartition { partition =>
      val producer = KafkaProducerCache.getProducer[K, V](producerConfig)
      partition
        .map(transformFunc)
        .foreach(record => producer.send(record))
    }
  }
}