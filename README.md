# Spark-Util 

language : `Scala` <br>
Scala   : `2.10` <br>
Spark   : `1.6.0` <br>
Hbase   : `1.2.0` <br>
 # Spark Kafka Util <br>
 * scala version 2.10 <br>
 spark version 1.3.0 1.6.0 <br>
 kafka version 0.8 <br>
 * 提供 对外的 ssc创建Dstream的方法。
 * 提供 对外的 ssc利用conf创建Dstream的方法
 * 提供 使用direct方式读取kafka数据的方法
 * 提供 "kafka.last.consum" -> "last"/"consum" 参数，来动态决定获取kafka数据是从last还是从消费点开始
 * 提供 fromoffset 参数，决定从具体的offset开始获取数据
 * 提供 rdd的更新kafka offsets到zookeeper的方法
 * 提供 rdd 写入 Kafka 的方法
 # Spark Hbase Util <br>
 * scala version 2.10 <br>
   spark version 1.6.0 <br>
   hbase version 1.2.0
* spark scan hbase data to RDD
* spark RDD[T] get from hbase to RDD[U]
* spark RDD[T] write to hbase


