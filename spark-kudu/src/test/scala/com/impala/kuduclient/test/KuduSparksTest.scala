package com.impala.kuduclient.test

import org.apache.kudu.spark.kudu._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.conf.Configuration
import org.apache.kudu.client.RowResult
import org.apache.spark.sql.SQLContext
import java.io.Serializable
import org.apache.commons.net.util.Base64
import java.io.ByteArrayOutputStream
import org.apache.kudu.mapreduce.KuduTableOutputFormat
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.client.KuduPredicate
import org.apache.kudu.Type
import org.apache.kudu.mapreduce.KuduTableInputFormat
import org.apache.kudu.mapreduce.SparkKuduTableInputFormat

object KuduSparksTest {
  System.setProperty("hadoop.home.dir", "F:\\eclipse\\hdplocal2.6.0")
  val kuduMaster = "kylin-master2"

  val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("Test"))
  val sparksql = new SQLContext(sc)
  import sparksql.implicits._
  val a = new KuduContext(kuduMaster, sc)
  def main(args: Array[String]): Unit = {
    writetoKudu
  }
  def writetoKudu() {
    val tableName = "impala::default.student"
    val rdd = sc.parallelize(Array("1", "4", "6")).map { n => STU(n.toInt, n) }
    val data = rdd.toDF()
    a.insertRows(data, tableName)//如果主键存在就会报错
    a.insertIgnoreRows(data, tableName)//主键如果存在就跳过
    println("------------------")
  }
  def getKuduRDD() {

    val tableName = "impala::default.kudu_pc_log"
    val columnProjection = Seq("siteid", "uid")
    val kp = KuduPredicate.newComparisonPredicate(new ColumnSchemaBuilder("siteid", Type.STRING).build(), KuduPredicate.ComparisonOp.EQUAL, "nVdTH0P1TkB")

    //这个完全可以自己实现
    val df = a.kuduRDD(sc, tableName, columnProjection,Array(kp))
    df.foreach { x => println(x.mkString(",")) }

  }
  /**
   * 使用过滤条件来获取数据存成rdd
   */
  def newKuduRDD() {

    val tableName = "impala::default.kudu_pc_log"
    val kp = KuduPredicate.newComparisonPredicate(new ColumnSchemaBuilder("siteid", Type.STRING).build(), KuduPredicate.ComparisonOp.EQUAL, "nVdTH0P1TkB")
    val conf = new Configuration
    conf.set("kudu.mapreduce.master.address", kuduMaster) //设置master
    conf.set("kudu.mapreduce.input.table", tableName) //设置表
    conf.set("kudu.mapreduce.column.projection", "siteid,uid") //设置返回字段
    conf.set("kudu.mapreduce.encoded.predicates", base64EncodePredicates(List(kp))) //设置过滤
    val rdd = sc.newAPIHadoopRDD(conf, classOf[SparkKuduTableInputFormat], classOf[NullWritable], classOf[RowResult])
    rdd.foreach { x => println(x._2.getString("siteid")) }
  }
  def base64EncodePredicates(predicates: List[KuduPredicate]) = {
    val baos = new ByteArrayOutputStream();
    for (predicate <- predicates) {
      predicate.toPB().writeDelimitedTo(baos);
    }
    Base64.encodeBase64String(baos.toByteArray());
  }
  case class STU(id: Int, name: String)
  case class PCLOG(deliverytime: String, siteid: String, plan: String, activity: String, uid: String) extends Serializable
}