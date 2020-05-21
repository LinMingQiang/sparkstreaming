package org.apache.spark.kafka.writer

import java.util.Properties
import org.apache.kafka.clients.producer.KafkaProducer
import scala.collection.mutable
private[spark] object KafkaProducerCache {
  private lazy val producers =
    mutable.HashMap.empty[Properties, KafkaProducer[_, _]]

  /**
    * Retrieve a [[KafkaProducer]] in the cache or create a new one
    * @param producerConfig properties for a [[KafkaProducer]]
    * @return a [[KafkaProducer]] already in the cache
    */
  def getProducerCache[K, V](
      producerConfig: Properties): KafkaProducer[K, V] = {
    producers
      .getOrElse(producerConfig, {
        val producer = new KafkaProducer[K, V](producerConfig)
        producers(producerConfig) = producer
        producer
      })
      .asInstanceOf[KafkaProducer[K, V]]
  }

  /**
    * @author LMQ
    * @func 在低版本的kafka，如果不close ，会导致数据少发的情况
    */
  def getProducer[K, V](producerConfig: Properties): KafkaProducer[K, V] = {
    new KafkaProducer[K, V](producerConfig)
  }
}
