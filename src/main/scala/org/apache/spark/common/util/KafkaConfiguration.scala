package org.apache.spark.common.util

import java.io.Serializable
import java.util.HashMap
trait KafkaConfiguration extends Configuration{
  private var conf:HashMap[String,String]=new HashMap[String,String]
  var kafkaParams: Map[String, String]=null
  var topics:Set[String]=null
  var groupid:String=null
  def setKafkaParams(kp: Map[String, String]){
    kafkaParams=kp
  }
  def getKafkaParams()={
    kafkaParams
  }
  def setGroupID(g:String){
    this.groupid=g
  }
  def getGoupid()={
    groupid
  }
  def kpIsNull:Boolean=kafkaParams==null
  def tpIsNull:Boolean=topics==null

  def setTopics(topics:Set[String]){
    this.topics=topics
  }
  def setTopics(topic:String){
    setTopics(topic.split(",").toSet)
  }
  
}