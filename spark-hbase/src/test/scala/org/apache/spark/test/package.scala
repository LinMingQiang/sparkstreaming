package org.apache.spark
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
package object test{
   def makeGet(x: String): Get = {
    new Get(x.getBytes)
  }
  def convertResult(result: Result) = {
    ""
  }

}