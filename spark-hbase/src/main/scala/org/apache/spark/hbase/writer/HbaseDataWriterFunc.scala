package org.apache.spark.hbase.writer

import org.apache.spark.rdd.RDD
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable

class HbaseDataWriterFunc[T](rdd:RDD[T]) {
  
  def writeToHbase(
      zk:String,
      table:String,
      conf:Configuration,
      f:T=>(ImmutableBytesWritable, Put)){
    val hconf=createJob(zk,table, conf)
    rdd.map( f )
       .saveAsNewAPIHadoopDataset(hconf)
  }
}