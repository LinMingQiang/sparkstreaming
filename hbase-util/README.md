# Hbase util 
* 操作 Hbase 的工具类，查询hbase表的region信息，用于手动split 某些过大的 </br>
# Example
```
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


```