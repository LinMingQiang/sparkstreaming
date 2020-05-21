package com.spark.test

import org.apache.spark.core.SparkKafkaContext
import org.apache.spark.SparkConf
object SparkKafkaContextTest {
  def main(args: Array[String]): Unit = {
    val groupId = "test"
    val brokers = "kafka-2:9092,kafka-1:9092,kafka-3:9092"
    val kp = SparkKafkaContext.getKafkaParam(
      brokers,
      groupId,
      "consum", // last/consum/custom/earliest
      "earliest" //wrong_from
    )
    val topics = Set("smartoutdooradsdeliverylog") // Set("mobileadsdeliverylog", "mobileadsclicklog", "app_conversion")//
    val skc = new SparkKafkaContext(
      kp,
      new SparkConf()
        .setMaster("local")
        .set(SparkKafkaContext.MAX_RATE_PER_PARTITION, "100")
        .setAppName("SparkKafkaContextTest")
    )
    skc.sparkcontext.setLogLevel("ERROR")
    val kafkadataRdd =
      skc.kafkaRDD[((String, Int, Long), String)](topics, msgHandle2) //根据配置开始读取
    kafkadataRdd.foreach { println } //print offset range
    //kafkadataRdd.updateOffsets(kp)                  //update offset

  }
}
