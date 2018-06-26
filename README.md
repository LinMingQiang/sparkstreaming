# Spark-Util 
大数据生态中有许多的组件，spark作为大数据的主流技术，在实际开发中spark会经常更其他组件进行结合开发。但是spark官方没有响应的工具包来支持。
这里主要是自己封装了一些常用的组件与spark结合。对于一些简单需求，或者新手来说可能比较适合。以下代码全部在生产线上使用过了并且稳定运行。bug的话暂时没发现。如果你使用过程中有发现什么bug或者有新的idea，可以留言------LinMingQiang  <br>
language : `Scala` <br>
Scala   : `2.10` <br>
Kafka   ：`0.8.0+` <br>
Spark   : `1.3.0+` <br>
Hbase   : `1.0.0+` <br>
 # Spark Kafka Util <br>
 * sparkstreaming 使用 direct方式读取kafka，不需要自己在手动维护offset。已经封装好了。提供许多配置参数来控制读取kafka数据的方式
 * 支持spark 1.3+ 和 kafka 0.8+
 * 封装了许多使用的方法。 <br>
https://github.com/LinMingQiang/spark-kafka
 # Spark Hbase Util <br>
 * spark读取和写入hbase <br>
 https://github.com/LinMingQiang/spark-util/tree/spark-hbase

# Spark ES Util  <br>
* spark读取es和写入es  <br>
https://github.com/LinMingQiang/spark-util/tree/spark-es

# Spark Kudu  <br>
* spark读取kudu数据 <br>
https://github.com/LinMingQiang/spark-util/tree/spark-kudu

# Splunk  <br>
* 日志监控工具Splunk的安装和使用 <br>
https://github.com/LinMingQiang/spark-util/tree/splunk

# flink kafka
* flink读取kafka数据，并结合hbase实现一个简单的wc的实例 <br>
https://github.com/LinMingQiang/spark-util/tree/flink-kafka

# Kafka Util
* 操作kafka 的工具类，提供按天来记录topic的offset，主要用于当天重算，小时重算等功能  <br>
https://github.com/LinMingQiang/spark-util/tree/kafka-util

# Hbase Util
* 操作 Hbase 的工具类，查询hbase表的region信息，用于手动split 某些过大的region  <br>
https://github.com/LinMingQiang/spark-util/tree/hbase-util


# database-util
* 提供各个数据库的连接工具  <br>
https://github.com/LinMingQiang/spark-util/tree/database-util

# es shade
* es shade完之后的jar，解决es和spark，hadoop相关包冲突的问题   <br>
https://github.com/LinMingQiang/spark-util/tree/es-shaed

# rabbitmq util
* 一个用来发送和消费mq消息的工具类   <br>
https://github.com/LinMingQiang/spark-util/tree/rabbitmq-util

