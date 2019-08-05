package org.apache.spark.kudu.olap

import org.apache.spark.rdd.RDD

trait kuduImplicitTrait {
  implicit def rddtoDF[T](rdd:RDD[T])=new RddToDataFram(rdd)
}