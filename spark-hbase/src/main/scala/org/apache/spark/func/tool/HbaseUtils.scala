package org.apache.spark.func.tool

import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat

trait HbaseUtils {
  def createJob(table: String, conf: Configuration): Configuration = {
    val job = Job.getInstance(conf, this.getClass.getName.split('$')(0))
    job.setOutputFormatClass(classOf[TableOutputFormat[String]])
    job.getConfiguration
  }
  def createJob(
      zk:String,
      table: String, 
      conf: Configuration): Configuration = {
    conf.set(TableOutputFormat.OUTPUT_TABLE, table)
    conf.set("hbase.zookeeper.quorum ",zk)
    conf.set("zookeeper.znode.parent","/hbase")
    val job = Job.getInstance(conf, this.getClass.getName.split('$')(0))
    job.setOutputFormatClass(classOf[TableOutputFormat[String]])
    job.getConfiguration
  }
}