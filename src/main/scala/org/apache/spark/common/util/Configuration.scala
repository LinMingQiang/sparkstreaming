package org.apache.spark.common.util

import java.util.HashMap

trait Configuration extends Serializable{
  private var conf:HashMap[String,String]=new HashMap[String,String]
  var kafkaParams: Map[String, String]=null
  var topics:Set[String]=null
  def containsKey(key:String)=conf.containsKey(key)
  def getString(key:String,default:String)={
    if(conf.containsKey(key)) conf.get(key)
    else default
  }
  def getInt(key:String,default:Int)={
    if(conf.containsKey(key)) conf.get(key).toInt
    else default
  }
  def set(key:String,value:String){
    conf.put(key, value)
  }
  def setInt(key:String,value:Int){
    conf.put(key, value+"")
  }
  def get(key:String,default:String)={
    getString(key,default)
  }
  def get(key:String)={
    getString(key,"")
  }
  def getBoolean(key:String,d:Boolean)={
     if(conf.containsKey(key)) conf.get(key).toBoolean
     else d
  }
  def setKafkaParams(kp: Map[String, String]){
    kafkaParams=kp
  }
  def getKafkaParams()={
    kafkaParams
  }
  def kpIsNull:Boolean=kafkaParams==null
  def tpIsNull:Boolean=topics==null
  def getKV()={
    conf
  }
  def setTopics(topics:Set[String]){
    this.topics=topics
  }
}