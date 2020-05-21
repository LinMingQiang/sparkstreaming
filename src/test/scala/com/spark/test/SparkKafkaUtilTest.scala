package com.spark.test

import org.apache.spark.core.SparkKafkaContext
import org.apache.spark.kafka.util.SparkKafkaUtil

object SparkKafkaUtilTest {
  def main(args: Array[String]): Unit = {
    val brokers = "kafka-2:9092,kafka-1:9092,kafka-3:9092"
    val groupid = "test"
    //val topics = "MST_417".split(",").toSet
    val topics = Set("smartoutdooradsdeliverylog") //Set("smartoutdooradsdeliverylog")//Set("mobileadsdeliverylog", "mobileadsclicklog", "app_conversion")
    val kp = SparkKafkaContext.getKafkaParam(
      brokers,
      groupid,
      "consum", // last/consum
      "earliest" //wrong_from
    )
    val sku = new SparkKafkaUtil(kp)
    //sku.updataOffsetToEarliest(topics, kp)
    //sku.getConsumerOffset(kp, groupid, topics).foreach(println)
    //sku.updataOffsetToLastest(topics, kp)
    //sku.getEarliestOffsets(topics, kp)
    val cu = sku.getConsumerOffset(groupid, topics)
    val la = sku.getLatestOffsets(topics)
    cu.foreach {
      case (tp, l) =>
        println(l, la(tp))

    }
    // sku.updataOffsetToLastest(topics)
    //sku.updateConsumerOffsets(la.map{case(tp,l)=> tp -> (l-100)})

    //最早的
    //val offsets=s"""mac_probelog,0,480645996|mac_probelog_wifi,1,16622577|mac_probelog_wifi,4,16261842|mac_probelog_wifi,3,16036652|mac_probelog_wifi,5,17184533|mac_probelog_wifi,0,16416957|mac_probelog_wifi,2,15432487"""
    //sku.updataOffsetToCustom(kp, offsets)
  }
}
