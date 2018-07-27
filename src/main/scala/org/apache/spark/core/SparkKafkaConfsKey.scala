package org.apache.spark.core

trait SparkKafkaConfsKey {
   val GROUPID="group.id"
   val BROKER="metadata.broker.list"
   val BOOTSTRAP="bootstrap.servers"
   val SERIALIZER="serializer.class"
   /*
    * 如果groupid不存在或者过期选择从last还是从earliest开始
    */
   val WRONG_GROUP_FROM="wrong.groupid.from"
   /*
    * 从last还是从consumer开始
    */
   val CONSUMER_FROM="kafka.consumer.from"
   
   val KAFKAOFFSET="kafka.offset"
   val MAX_RATE_PER_PARTITION="spark.streaming.kafka.maxRatePerPartition"
     /**
   * @author LMQ
   * @description 获取kafka的配置，一般不做特殊的配置，用这个就够了
   * @param brokers :kafka brokers
   * @param groupid :kafka groupid
   * @param consumer_from :kafak 从哪开始消费  
   * 			last ： 从最新数据开始
   * 			earliest ：从最早数据开始
   *      consum： 从上次消费点继续
   *      custom：自定义消费
   * @param :wrong_from ：如果kafka的offset出现问题，导致你读不到，这里配置是从哪里开始读取
   * @param kafkaoffset ： 自定义offset  
   */
  def getKafkaParam(
    brokers: String,
    groupid: String,
    consumer_from: String,
    wrong_from: String,
    kafkaoffset: String="") = {
    Map[String, String](
      BROKER -> brokers,
      BOOTSTRAP->brokers,
      SERIALIZER -> "kafka.serializer.StringEncoder",
      GROUPID -> groupid,
      WRONG_GROUP_FROM -> wrong_from, //EARLIEST
      CONSUMER_FROM -> consumer_from, //如果是配置了CUSTOM。必须要配一个 kafka.offset的参数
      KAFKAOFFSET -> kafkaoffset)
  }
}