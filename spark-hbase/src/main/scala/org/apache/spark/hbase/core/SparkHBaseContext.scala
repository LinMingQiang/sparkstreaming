package org.apache.spark.hbase.core

import org.apache.spark.SparkContext
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SerializableWritable
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil
import org.apache.hadoop.hbase.mapreduce.IdentityTableMapper
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.client.Get
import scala.collection.JavaConversions._
import org.apache.spark.api.java.JavaSparkContext.fakeClassTag
class SparkHBaseContext(
    @transient val sc: SparkContext,
    zk:String
    ) extends Serializable {
  /**
   * @author LMQ
   * @time 2018-08-20
   * @func 根据scan扫描出hbase数据
   */
  def scanHbaseRDD[T: ClassTag](
    tableName: String,
    scan: Scan,
    f: ((ImmutableBytesWritable, Result)) => T): RDD[T] = {
    val job = getHbaseBulkJob(tableName)
    TableMapReduceUtil.initTableMapperJob(tableName, scan, classOf[IdentityTableMapper], null, null, job)
    sc.newAPIHadoopRDD(job.getConfiguration,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]).map(f)
  }
  /**
   * @author LMQ
   * @time 2018-04-08
   * @func 获取hbase读取的job
   */
  def getHbaseBulkJob(
    tableName: String) = {
    val conf=sc.hadoopConfiguration
    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    conf.set("hbase.zookeeper.quorum", zk)
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    new Job(conf)
  }
  /**
   * @author LMQ
   * @time 2018-04-08
   * @func 读取hbase的所有数据
   */
  def hbaseRDD[T: ClassTag](
    tableName: String,
    f: ((ImmutableBytesWritable, Result)) => T): RDD[T] = {
    val job = getHbaseBulkJob(tableName)
    sc.newAPIHadoopRDD(job.getConfiguration(),
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]).map(f)
  }
  /**
   * @author LMQ
   * @time 2018-04-08
   * @func 读取hbase的所有数据
   */
  def hbaseRDD[T: ClassTag](
    tableName: String,
    conf: Configuration,
    f: ((ImmutableBytesWritable, Result)) => T): RDD[T] = {
    var job: Job = new Job(conf)
    sc.newAPIHadoopRDD(job.getConfiguration(),
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]).map(f)
  }
  /**
   * @author LMQ
   * @time 2018-04-08
   * @func 批量gethbase的数据
   */
  def batchGetRDD[T, U](
    tableName: String,
    batchSize: Int, //get batch
    rdd: RDD[T],
    makeGet: (T) => Get,
    convertResult: (Result) => U): RDD[U] = {
    rdd.mapPartitions[U] { it =>
      val table = HbaseConnectionCache.getTable(zk,tableName)
      it.grouped(batchSize).flatMap { ts =>
        table.get(ts.map { makeGet(_) }.toList)
          .filter { x => x != null && !x.isEmpty() }
      }.map { convertResult(_) }
    }(fakeClassTag[U])
  }
  /**
   * @author LMQ
   * @time 2018-04-08
   * @func 批量gethbase的数据
   */
  def batchGetRDD[T, U](
    tableName: String,
    rdd: RDD[T],
    makeGet: (T) => Get,
    convertResult: (Result) => U): RDD[U] = {
    batchGetRDD(tableName, 1000, rdd, makeGet, convertResult)
  }
}