# Spark-Util 
There are many components in bigdata ecology. As the mainstream technology of bigdata, spark often combines with other components in actual development. But spark official did not respond to the toolkit to support it.
Here is mainly encapsulated some common components with spark. For some simple needs, or newcomers may be more suitable. The following code is used in the production line and runs smoothly. Bug has not been discovered for the time being. If you use any bug or new idea in the process, you can leave a message.------LinMingQiang  <br>
## Support
---
|                    | scala version      |Kafka version       | hbase 1.0+         | es   2.3.0         |kudu  1.3.0         |
|:------------------:|:------------------:|:------------------:|:------------------:|:------------------:|:------------------:|
| **spark 1.3.x**    | 2.10               | 0.8+               | :smiley: | :smiley: | :smiley: |
| **spark 1.6.x**    | 2.10               | 0.8+               | :smiley: | :smiley: | :smiley: |
| **spark 2.0.x**    | 2.11               | 0.8+               | :smiley: | :smiley: | :smiley: |

 # Spark Kafka Util <br>
 * sparkstreaming 使用 direct方式读取kafka，不需要自己在手动维护offset。已经封装好了。提供许多配置参数来控制读取kafka数据的方式
 * 支持spark 1.3+ 和 kafka 0.8+
 * 提供了spark2.x kafka 0.10+的版本支持（这两个版本较之前的有大的变化）
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

