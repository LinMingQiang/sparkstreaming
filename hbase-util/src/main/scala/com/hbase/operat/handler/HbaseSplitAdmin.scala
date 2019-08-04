package com.hbase.operat.handler

import org.apache.hadoop.hbase.client.Admin
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import scala.collection.JavaConversions._
import org.apache.hadoop.hbase.util.Bytes
import java.util.HashMap
import org.apache.hadoop.hbase.RegionLoad
class HbaseSplitAdmin(zk: String) {
  val conf = HBaseConfiguration.create();
  conf.set("zookeeper.znode.parent", "/hbase");
  conf.set("hbase.zookeeper.quorum", zk);
  conf.set("hbase.zookeeper.property.clientPort", "2181");
  val connection = ConnectionFactory.createConnection(conf);
  var admin = connection.getAdmin();
  /**
   * @author LMQ
   * @func 获取某个table的regionname
   */
  def getTableRegionName(table: String) = {
    admin.getTableRegions(TableName.valueOf(table))
  }
  /**
   * @author LMQ
   * @func 获取hbase下所有的region的具体信息
   * @time 20180504
   */
  def getAllRegionInfo() = {
    val re = new HashMap[String, RegionLoad]
    val clusterStatus = admin.getClusterStatus();
    clusterStatus
      .getServers()
      .foreach { serverName =>
        val serverLoad = clusterStatus.getLoad(serverName)
        serverLoad
        .getRegionsLoad
        .entrySet()
        .foreach { entry =>
          val region = Bytes.toString(entry.getKey());
          val regionLoad = entry.getValue();
          re.put(region, regionLoad)
        }
      }
    re
  }
  /**
   * @author LMQ
   * @func 获取所有table的region具体信息
   * @time 20180504
   */
  def getAllTableregionInfo()={
    getAllRegionInfo
    .toList
    .map{case(region,load)=>
      val tablename = region.split(",")(0)
    (tablename,load)
    }.groupBy(_._1)
    .map{case(table,list)=>
    (table,list.map(_._2))
    }
  }
  def splitRegion(regionName:String){
    admin.splitRegion(regionName.getBytes)
  }
}