package org.apache.spark.streaming.kafka
/**
 * @author LMQ
 * @description 主要是为非spark包提供的工具类。如果你不想用spark，只是想要操作kafka的offset。可以直接使用这个object
 */
class KafkaUtil(kp:Map[String,String]) extends KafkaSparkTool{
  instance(kp)
}