package com.spark.test

import org.apache.spark.core.SparkKafkaContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.streaming.core.StreamingDynamicContext
import kafka.serializer.StringDecoder
import kafka.message.MessageAndMetadata
import org.apache.spark.streaming.kafka.KafkaDataRDD
import org.slf4j.LoggerFactory
import org.apache.log4j.BasicConfigurator
import org.apache.log4j.PropertyConfigurator
import java.util.Date

object StreamingDynamicContextTest {
  PropertyConfigurator.configure("conf/log4j.properties");
  def msgHandle =
    (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message)
  def main(args: Array[String]): Unit = {

    val brokers = "kafka-2:9092,kafka-1:9092,kafka-3:9092"
    val groupId = "test"
    val kp = SparkKafkaContext.getKafkaParam(
      brokers,
      groupId,
      "consum", // last/consum/custom/earliest
      "last" //wrong_from
    )
    val topics = Set("mobileadsclicklog") //smartadsdeliverylog
    val skc = new SparkKafkaContext(
      kp,
      new SparkConf()
        .setMaster("local")
        .set("spark.streaming.backpressure.enabled", "true") //是否启动背压
        .set("spark.streaming.backpressure.pid.minRate", "1") //最低速率
        .set(SparkKafkaContext.MAX_RATE_PER_PARTITION, "100")
        .setAppName("SparkKafkaContextTest")
    )
    val sskc = new StreamingDynamicContext(skc, Seconds(10))
    val kafkastream = sskc.createKafkaDstream[
      String,
      String,
      StringDecoder,
      StringDecoder,
      (String, String)
    ](topics, msgHandle)

    kafkastream.foreachRDD {
      case (rdd) =>
        println("################ start ##################")
        val st = new Date().getTime
        val count = rdd.count
        rdd.take(1).foreach { x =>
          println(x)
        }
        //rdd.map(x => x._2).collect().foreach { x => }
        println("耗时(schedule + jobexe)： ", (new Date().getTime - st))
        rdd.getRDDOffsets().foreach(println)
        //rdd.updateOffsets()
        println("################ END ##################")

        count > 5 //是否马上执行下个批次。否则就等到下一批次时间到来 。 （这里设为，如果kafka还有数据就立即执行下一批次，否则等待10s）
    }
    sskc.start()
  }
}
