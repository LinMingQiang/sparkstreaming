package org.apache.spark.common.util

import java.util.HashMap

trait Configuration extends Serializable{
  private var conf:HashMap[String,String]=new HashMap[String,String]
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
  def apply(key:String)={
    getString(key,"")
  }
    def getKV()={
    conf
  }
  def getBoolean(key:String,d:Boolean)={
     if(conf.containsKey(key)) conf.get(key).toBoolean
     else d
  }
}