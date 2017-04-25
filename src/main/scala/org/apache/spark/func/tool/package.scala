package org.apache.spark.func

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.rdd.RDD
import org.apache.spark.kafka.writer.rddKafkaWriter

package object tool {
  implicit def sscFunc(ssc: StreamingContext)=new SSCFuncClass(ssc)
  
  implicit def rddFunc[T](rdd:RDD[T])=new rddkafkaFunClass(rdd)
  implicit def writeKafka[T](rdd:RDD[T])=new rddKafkaWriter(rdd)
}