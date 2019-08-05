package org.apache.spark.kudu.olap

import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

class RddToDataFram[T](rdd:RDD[T]) {
  def transtoDF[R:ClassTag](transfor:(T)=>R)={
    rdd.mapPartitions{x=>x.map {transfor}.filter { _ !=null } }
  }
}