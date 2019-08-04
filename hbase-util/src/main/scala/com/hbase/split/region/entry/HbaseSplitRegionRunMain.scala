package com.hbase.split.region.entry
/**
 * @time 2018-04-13
 * @func 主要功能是为了 split 大块的 region。 
 * 	适合使用的场景就是： 定时地去切分hbase的region。防止region过大而影响hbase的整体性能
 * @author LMQ
 */
object HbaseSplitRegionRunMain {
  def main(args: Array[String]): Unit = {
    println(">>>>>>>>>>>>>>")
  }
}