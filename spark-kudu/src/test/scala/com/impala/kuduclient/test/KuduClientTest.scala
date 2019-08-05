package com.impala.kuduclient.test

import org.apache.kudu.client.KuduClient
import org.apache.kudu.client.KuduClient.KuduClientBuilder
import scala.collection.JavaConversions._
import org.apache.kudu.client.KuduPredicate
import org.apache.kudu.client.KuduScanToken
object KuduClientTest {
  def main(args: Array[String]): Unit = {

  }
  def writeKudu() {

    
    
    
  }
  def scanKudu() {
    val client = new KuduClientBuilder("kylin-master2").build()
    val table = client.openTable("impala::default.kudu_pc_log")
    client.getTablesList.getTablesList.foreach { println }
    val schema = table.getSchema();
    val kp = KuduPredicate.newComparisonPredicate(schema.getColumn("siteid"), KuduPredicate.ComparisonOp.EQUAL, "nVdTH0P1TkB")
    val scanner = client.newScanTokenBuilder(table)
      .addPredicate(kp)
      .limit(100)
      .build()
    val token = scanner.get(0)
    val scan = KuduScanToken.deserializeIntoScanner(token.serialize(), client)
    while (scan.hasMoreRows()) {
      val results = scan.nextRows()
      while (results.hasNext()) {
        val rowresult = results.next();
        println(rowresult.getString("siteid"))
      }
    }

  }
}