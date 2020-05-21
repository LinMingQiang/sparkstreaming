package org.apache.spark.func.tool

import org.apache.spark.rdd.RDD
import org.apache.spark.kafka.writer.RDDKafkaWriter

trait KafkaImplicittrait {
  /**
   * @author LMQ
   * @desc write kafka的隐式转换
   */
  implicit def writeKafka[T](rdd:RDD[T])=new RDDKafkaWriter(rdd)
}