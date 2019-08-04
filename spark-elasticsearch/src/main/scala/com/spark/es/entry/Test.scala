//package com.spark.es.entry
//
//import java.util.LinkedHashMap
//
//import com.alibaba.fastjson.JSON
//import com.spark.es.util.ElasticsearchManagerTool
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.io.NullWritable
//import org.apache.spark.{SparkConf, SparkContext}
//import org.elasticsearch.hadoop.mr.{EsInputFormat, LinkedMapWritable}
//import org.elasticsearch.index.query.QueryBuilders
//import org.elasticsearch.spark._
//
//import scala.collection.JavaConversions._
//
//object Test {
//  val address = "192.168.10.115,192.168.10.110,192.168.10.81"
//  val clusterName = "zhiziyun"
//  lazy val client = ElasticsearchManagerTool.getESClient(address, clusterName)
//  def main(args: Array[String]): Unit = {
//    val confs = new SparkConf()
//      .setAppName("esRDDtest")
//      .setMaster("local")
//    confs.set("es.nodes", "192.168.10.115,192.168.10.110,192.168.10.81")
//    confs.set("es.port", "9200")
//    confs.set("es.mapping.date.rich", "false") //关闭es的date。如果报日期的错误。
//    confs.set("cluster.name", clusterName)
//    val sc = new SparkContext(confs)
//    func1(sc)
//
//  }
//  def func1(sc: SparkContext) {
//    val query = s"""{"query":${getQuery}}"""
//    println(query)
//    val conf = new Configuration
//    conf.set("es.nodes", "192.168.10.115,192.168.10.110,192.168.10.81")
//    conf.setInt("es.port", 9200)
//    conf.set("cluster.name", clusterName)
//    conf.set("es.resource", "dataexchange_device_tags/deviceTags")
//    conf.set("es.mapping.date.rich", "false") //关闭es的date。如果报日期的错误。
//    conf.set("es.query", query);
//    sc.newAPIHadoopRDD(conf,
//                       classOf[EsInputFormat[NullWritable, LinkedMapWritable]],
//                       classOf[NullWritable],
//                       classOf[LinkedMapWritable])
//      .foreach(println)
//  }
//  def func2(sc: SparkContext) {
//    val query = s"""{"query":${getQuery}}"""
//    println(query)
//    //val query = s"""{"query":{"match":{"_id":"http%3A%2F%2Fbbs.zhan.com%2Fthread-337326-1-1.html"}}}"""
//    sc.esRDD("dataexchange_device_tags/deviceTags", query)
//      .foreach {
//        case (id, ss) =>
//          ss.foreach {
//            case (key, value) =>
//              if (value
//                    .isInstanceOf[scala.collection.AbstractTraversable[Any]]) {
//                val l =
//                  value.asInstanceOf[scala.collection.AbstractTraversable[Any]]
//                val arr = JSON.parseArray("[]")
//                l.foreach { x =>
//                  if (x.isInstanceOf[LinkedHashMap[_, _]]) {
//                    val a = x.asInstanceOf[LinkedHashMap[String, Any]]
//                    val js = JSON.parseObject("{}")
//                    a.foreach { case (key, value) => js.put(key, value) }
//                    arr.add(js)
//                  } else if (x.isInstanceOf[String]) {
//                    arr.add(x.toString())
//                  } else println(">>>>>>>>>>>>.")
//
//                }
//              } else if (value.isInstanceOf[String]) {} else {
//                println(">>>>>>>>>", value.getClass.getName)
//              }
//            //val arr=JSONArray.fromObject(a)
//            //println(arr)
//          }
//
//      }
//  }
//
//  /**
//    * 拼接处一个query语句
//    *
//    */
//  def getQuery() = {
//    QueryBuilders
//      .matchQuery("probemac", "6001946955ff")
//      .toString()
//    //正则匹配
//    //QueryBuilders.regexpQuery("", "6001941c2d66")
//    //QueryBuilders.prefixQuery("probemac", "6001941c2d66")
//    //.toString()
//    /*QueryBuilders.rangeQuery("creattime")
//    .from("2018-01-03 17:37:47")
//    .to("2018-01-04 17:37:47")
//    .toString()*/
//    /*QueryBuilders
//      .andQuery(QueryBuilders.prefixQuery("_id", "9492bc7ab90b"))
//      .add(QueryBuilders.matchQuery("_id", "9492bc7ab90b"))
//      .add(QueryBuilders.rangeQuery("creattime").from("2018-01-03 17:37:47").to("2018-01-04 17:37:47"))
//      .toString()*/
//  }
//
//  def esTest() {
//    val r = client.prepareGet("sdr_urlinfo", "urlinfo", "abcedsa.afaf")
//    println(r.get.getSource)
//    val response = client
//      .prepareSearch("sdr_urlinfo")
//      .setTypes("urlinfo")
//      //.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
//      .setQuery(QueryBuilders.prefixQuery("url", "abcedsa.afaf")) // Query
//      //.setPostFilter(QueryBuilders.rangeQuery("age").from(12).to(18))     // Filter
//      .setFrom(0)
//      .setSize(60)
//      .setExplain(true)
//      .get();
//    println(response)
//
//  }
//}
