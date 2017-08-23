package org.apache.spark.core

trait SparkKafkaConfsKey {
   /*
   * 如果groupid不存在或者过期选择从last还是从earliest开始
   */
   val NEWGROUP_LAST_EARLIEST="newgroup.last.earliest"
   /*
    * 从last还是从consumer开始
    */
   val LAST_CONSUMER="kafka.last.consum"
}