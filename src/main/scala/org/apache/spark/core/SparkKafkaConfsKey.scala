package org.apache.spark.core

trait SparkKafkaConfsKey {
   /*
   * 如果groupid不存在或者过期选择从last还是从earliest开始
   */
   val WRONG_FROM="wrong.groupid.from"
   val GROUPID="group.id"
   val BROKER="metadata.broker.list"
   val SERIALIZER="serializer.class"
   /*
    * 从last还是从consumer开始
    */
   val CONSUMER_FROM="kafka.consumer.from"
   
   val KAFKAOFFSET="kafka.offset"
}