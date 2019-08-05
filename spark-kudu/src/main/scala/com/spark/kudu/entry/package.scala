package com.spark.kudu

import org.apache.spark.kudu.olap.kuduImplicitTrait
import kafka.message.MessageAndMetadata
import org.slf4j.LoggerFactory
import org.apache.spark.kudu.olap.caseclassTrait
import java.util.Random
package object entry extends kuduImplicitTrait
    with caseclassTrait with SmartSQL {
  val LOG = LoggerFactory.getLogger("kudureport")
  val zookeeper = "solr2.zhiziyun.com,solr1.zhiziyun.com,mongodb3"
  val brokers = "kafka1:9092,kafka2:9092,kafka3:9092"
  val kudumaster = "kylin-master2"
  val radom=new  Random();
  //val intopics = "smartadsdeliverylog,smartadsclicklog,mobileadsdeliverylog,mobileadsclicklog,app_conversion"
 val intopics = "smartadsdeliverylog,smartadsclicklog"
  def msgHandle = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message)

  def transPC(d: (String, String)): smartTable = {
    d._1 match {
      case "smartadsdeliverylog" => smartdelivery(d._2)
      case "smartadsclicklog"    => smartclick(d._2)
      case _                     => null
    }
  }
  def transMobile(d: (String, String)): mobileTable = {
    d._1 match {
      case "mobileadsdeliverylog" => mobiledelivery(d._2)
      case "mobileadsclicklog"    => mobileclicks(d._2)
      case "app_conversion"       => mobileapporders(d._2)
      case _                      => null
    }
  }
  def mobiledelivery(str: String): mobileTable = {
    null 
  }
  def mobileclicks(str: String): mobileTable = {
    null
  }
  def mobileapporders(str: String): mobileTable = {
    null
  }
  def smartdelivery(str: String): smartTable = {
    val arr = str.split(",")
    val statdate = arr(0).substring(0, 10)
    val hour = arr(0).substring(11, 13)
    val siteid = arr(14)
    val plan = arr(25)
    val unit = arr(11)
    val activity = arr(26).trim
    val channel = arr(6).trim
    val creative = arr(12).trim //素材
    val slot = arr(10).trim
    val pubdomain = arr(8).trim
    val city = arr(2)
    val price = arr(4).toDouble
    val vc = arr(27).trim
    var vcs = arr(28).trim.replaceAll("\n", "")
    if (plan != "")
      smartTable(radom.nextLong(), statdate, hour, siteid, plan, unit,
        activity, channel, slot, pubdomain, creative,
        vc, vcs, city, 1, 0, price)
    else null
  }
  def smartclick(str: String): smartTable = {
    val arr = str.split(",")
    val statdate = arr(0).substring(0, 10)
    val hour = arr(0).substring(11, 13)
    val siteid = arr(2)
    val channel = arr(7).trim //渠道
    val pubdomain = arr(9).trim
    val slot = arr(12).trim
    val unit = arr(13)
    val creative = arr(14).trim //素材
    val plan = arr(17)
    val city = arr(4)
    val activity = arr(18).trim
    val vc = arr(19).trim
    var vcs = arr(20).trim.replaceAll("\n", "")
    if (plan.nonEmpty)
      smartTable(radom.nextLong(), statdate, hour, siteid, plan, unit, activity,
        channel, slot, pubdomain, creative,
        vc, vcs, city, 0, 1, 0.0)
    else null
  }
}