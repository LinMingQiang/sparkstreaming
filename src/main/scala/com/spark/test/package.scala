package com.spark
import org.apache.spark.func.tool.SparkImplicitTool
import org.apache.spark.common.util.configurationKey
import kafka.message.MessageAndMetadata
import org.apache.spark.common.util.ConfigurationFactoryTool

package object test extends SparkImplicitTool
with configurationKey
with ConfigurationFactoryTool{
  val zookeeper=""
  val brokers=""
  
  
  def msgHandle = (mmd: MessageAndMetadata[String, String]) 
  => (mmd.topic, mmd.message)
}