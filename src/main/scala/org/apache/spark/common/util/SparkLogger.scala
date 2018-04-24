package org.apache.spark.common.util

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.HashMap
import scala.collection.JavaConversions._
class SparkLogger(val logName:String) {
  @transient private var log_ : Logger = null
  protected def log: Logger = {
    if (log_ == null) {
      log_ = LoggerFactory.getLogger(logName)
    }
    log_
  }
  def info(msg:String){
    if(log.isInfoEnabled()) log.info(msg)
  }
  def error(msg:String){
    if(log.isErrorEnabled()) log.error(msg)
  }
  def info[K<:Any,V<:Any](kv:java.util.Map[K,V]){
    kv.foreach(x=>log.info(x._1+"->"+x._2))
  }
}