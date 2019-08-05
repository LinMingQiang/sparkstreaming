package com.spark.kudu.entry
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.SQLContext
import java.text.SimpleDateFormat

/**
  * @author LMQ
  * @description 将sparkstreaming的数据写进kudu。同时使用impala生成OLAP报表存成kudu。
  *
  */
object SparkStreamKuduRunMain {
  val sim = new SimpleDateFormat("yyyy-MM-dd");
  def main(args: Array[String]): Unit = {
    val batchTime = args(0).toLong
    runJob(batchTime)
  }
  def runJob(time: Long) {
    val sc = new SparkContext(
      new SparkConf().setAppName("SparkStreamKuduRunMain"))
    val kuducontext = new KuduContext(kudumaster, sc)
    val sparksql = new SQLContext(sc)
    import sparksql.implicits._
    val topics = intopics.split(",").toSet
    var count = 0L
    val dataFrame = sc.parallelize(1 to 100).toDF()
    kuducontext.insertRows(dataFrame, "impala::default.smartadslog")
    KuduImpalaUtil.execute(rt_rtbreport_byslot_channel_plan("20190401"))
  }
}
