package com.spark.kudu.test
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaSparkStreamManager
import kafka.serializer.StringDecoder
import org.slf4j.LoggerFactory
import org.apache.spark.common.util.Configuration
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.func.tool._
import org.apache.log4j.PropertyConfigurator
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.SQLContext
import com.spark.kudu.entry.KuduImpalaUtil
import java.util.Date
import java.text.SimpleDateFormat
import org.apache.spark.core.StreamingKafkaContext

/**
 * @author LMQ
 * @description 将sparkstreaming的数据写进kudu。同时使用impala生成OLAP报表存成kudu。
 *
 */
object SparkStreamKuduTest {
  val sim=new SimpleDateFormat("yyyy-MM-dd");
   val LOG = LoggerFactory.getLogger("kudureport")
  PropertyConfigurator.configure("conf/log4j.properties")
  def main(args: Array[String]): Unit = {
    runJob
  }
  def runJob() {
    val sc = new SparkContext(new SparkConf().setMaster("local[5]").setAppName("Test"))
    val kuducontext = new KuduContext(kudumaster, sc)
    val sparksql = new SQLContext(sc)
    import sparksql.implicits._
    val ssc = new StreamingKafkaContext(sc, Seconds(15))
    var kp = Map[String, String](
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder",
      "group.id" -> "test",
      "newgroup.last.earliest" -> "earliest", //如果是新的group id 就从最新还是最旧开始
      "kafka.last.consum" -> "last")
    val topics = intopics.split(",").toSet
    val ds = ssc.createDirectStream[(String,String)](kp, topics, msgHandle)
var count=0L
    ds.foreachRDD { rdd =>
      val d=rdd.filter{x=>
        x._1 match{
          case"smartadsdeliverylog"=>x._2.split(",")(25).nonEmpty
          case"smartadsclicklog"=>x._2.split(",")(17).nonEmpty
          case _=> false
        }
        }.count
      LOG.info("初始满足条件的.."+d )
      val data = rdd.transtoDF(transPC).toDF()
      val dfcount=data.count
      LOG.info("dfcount.."+dfcount )
      count=count+dfcount
      LOG.info("目前总数.."+count )
      kuducontext.insertRows(data, "impala::default.smartadslog")
      val date=new Date()
      val s=date.getTime
      val statdate=sim.format(date)
      KuduImpalaUtil.execute(rt_rtbreport(statdate))
      KuduImpalaUtil.execute(rt_rtbreport_byhour(statdate))
      KuduImpalaUtil.execute(rt_rtbreport_byplan(statdate))
      KuduImpalaUtil.execute(rt_rtbreport_byactivity(statdate))
      KuduImpalaUtil.execute(rt_rtbreport_bycreative(statdate))
      KuduImpalaUtil.execute(rt_rtbreport_byplan_unit(statdate))
      KuduImpalaUtil.execute(rt_rtbreport_byplan_unit_creative(statdate))
      KuduImpalaUtil.execute(rt_rtbreport_byslot_channel_plan(statdate))
      KuduImpalaUtil.execute(rt_rtbreport_bydomain_channel_plan(statdate))
      KuduImpalaUtil.execute(rt_rtbreport_byhour_channel_plan(statdate))
      LOG.info(s"""time : ${new Date().getTime - s}""")
      ssc.updateRDDOffsets(kp, "test", rdd)
      LOG.info(">>>>>>>>>>>>>>>.." )
    }

    ssc.start()
    ssc.awaitTermination()
  }
}