package org.apache.spark.test

import org.apache.spark.hbase.core.SparkHBaseContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.client.Scan
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable

object HbaseUtilTest {
  val tablename = "test"
  val zk = "zk1,zk2,zk3"
  def f(r: (ImmutableBytesWritable, Result)) = {
    val re = r._2
    new String(re.getRow)
  }
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("tets")
    val sc = new SparkContext(conf)
    val hc = new SparkHBaseContext(sc, zk)
    hc.hbaseRDD(tablename, f).foreach { println }
    hc.scanHbaseRDD(tablename, new Scan(), f)
  }
  /**
   * @func 根据rowkey，批量获取rdd
   */
  def testBatchGetRDD(hc: SparkHBaseContext, rdd: RDD[String]) = {
    hc.batchGetRDD(tablename, rdd, makeGet, convertResult)
  }
}