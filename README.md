# Spark-Util 

language : `Scala` <br>
ScalaV   : `2.10` <br>
SparkV   : `1.6.0` <br>
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

 # Spark Hbase Util <br>
 * scala version 2.10 <br>


