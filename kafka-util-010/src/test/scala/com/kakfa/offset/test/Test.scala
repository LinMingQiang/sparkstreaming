package com.kakfa.offset.test
import java.util.Date
import java.text.SimpleDateFormat
import org.apache.log4j.PropertyConfigurator
import com.kafka.offset.record.KafkaOffsetUtil
import org.slf4j.LoggerFactory
object Test {
  val smp = new SimpleDateFormat("yyyyMMdd")
  PropertyConfigurator.configure("conf/log4j.properties")
  val LOG=LoggerFactory.getLogger("Test")
  //val zk="192.168.0.235,192.168.0.231,192.168.0.234"
  val zk="solr1,solr2,datanode37"
  val broker = "kafka1,kafka2,kafka3"
  val topics = Set("test","117--12")
  def main(args: Array[String]): Unit = {
    val groupid = "kafkadayoffset"
    val day = "20180423"
    val hour="11"
    var kafkaParams = Map[String, String](
      "metadata.broker.list" -> broker,
      "serializer.class" -> "kafka.serializer.StringEncoder",
      "group.id" -> groupid)
    val statTime=new Date().getTime
    val kafkaoffsetUtil = KafkaOffsetUtil(kafkaParams, zk)
    println(kafkaoffsetUtil.getAlltopics())
    
    /*val r=kafkaoffsetUtil.recordDayOffsetsToZK(day, topics)
    if(r.isRight){
      r.right.get.foreach(println)
    }*/
    //kafkaoffsetUtil.recordDayHourOffsetToZK(day,hour,null)
    /*val res = kafkaoffsetUtil.getDayHourOffsetsFromZK(day, hour,topics)
    if (res.isLeft) {
      LOG.info(res.left.get)
    } else res.right.get.foreach(println)
      LOG.info(">>>>>>>>>>>>>")*/
  }

}