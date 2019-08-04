package com.hbase.region.test

import com.hbase.operat.handler.HbaseSplitAdmin
import scala.collection.JavaConversions._
object Test {
  def main(args: Array[String]): Unit = {
    val admin = new HbaseSplitAdmin("zk1,zk2,zk3")
    admin
    .getTableRegionName("test")
    .foreach { hr => 
    println(hr.getRegionId,new String(hr.getStartKey),new String(hr.getEndKey))
    }
    admin
    .getAllTableregionInfo()
    .foreach {case (tablename, regionLoadlist) =>
      if(tablename=="test"){
        val regionNameAndSize = regionLoadlist
        .map{x=>(x.getNameAsString,x.getStorefileSizeMB)}
        //admin.splitRegion("regionName")
        println(tablename,regionNameAndSize)
      } 
    }

  }
}