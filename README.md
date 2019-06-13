# :tada:Spark-Util 

大数据生态圈中有许多组件，spark作为大数据的主流技术，在实际开发中经常会与其他组件结合使用的情况，但是官方没有比较完整的工具包。这里主要是封装了一些常用的组件，例如：Sparkstreaming-Kafka，Spark-hbase等，对于一些简单的需求项目比较适合，或者比较适合新人学习阅读。遇到bug的可以在此提交error反馈------LinMingQiang  <br>

There are many components in bigdata ecology. As the mainstream technology of bigdata, spark often combines with other components in actual development. But spark official did not respond to the toolkit to support it.
Here is mainly encapsulated some common components with spark. for example:spark.Sparkstreaming-Kafka，Spark-hbase. For some simple needs, or newcomers may be more suitable. The following code is used in the production line and runs smoothly. Bug has not been discovered for the time being. If you use any bug or new idea in the process, you can leave a message.------LinMingQiang  <br>

最近更新 ： <br> 
 封装 StreamingDynamicContext ：
    实现以下功能： 不使用streamingContext 来实现流式计算，因为streamingContext是严格的时间间隔执行job任务，当job时间远小于batchtime时，会有大量的时间是在sleep等待下一个批次执行的到来(具体可以看看streamingContext的源码);<br>
    StreamingDynamicContext 的设计借鉴了streamingContext的设计。但是在Job的提交上不使用Queue队列来appending堆积的job。当job执行完后，用户可以自己选择是否立刻执行下一个批次的计算，还是选择继续等待指定时长。<br><br>
     封装 StreamingKafkaContext ：你依然可以用 streamingContext来实现流式计算，词Api封装了读取kafka数据。<br><br>
   Encapsulation Streaming DynamicContext: Implement the following functions: Streaming DynamicContext is not used to implement streaming computing, because streaming Context is a strict time interval to perform job tasks, when job time is much less than batchtime, there will be a lot of time in sleep waiting for the next batch of execution (see the source code of streaming Context specifically), Streaming DynamicContext The design draws lessons from the design of streaming Context. But Job submissions do not use Queue queues to append stacked jobs. When the job is finished, the user can choose whether to perform the next batch immediately or to continue waiting for the specified length of time. <br>

## Support
---
|                    | scala version      |Kafka version       | hbase 1.0+         | es   2.3.0         |kudu  1.3.0         |SSL         |
|:------------------:|:------------------:|:------------------:|:------------------:|:------------------:|:------------------:|:------------------:|
| **spark 1.3.x**    | 2.10               | 0.8               | :ok_hand: | :star2: | :eggplant: |NO |
| **spark 1.6.x**    | 2.10               | 0.8               | :baby_chick: | :santa: | :corn: |NO |
| **spark 1.6.x**    | 2.10               | 0.10+               | :baby_chick: | :santa: | :corn: |YES |
| **spark 2.0.x**    | 2.10/2.11          | 0.10+               | :smiley: | :cherries: | :peach: |YES |

---
## POINT
-------------------
> 支持动态调节 streaming 的 批次间隔时间 （不同于sparkstreaming 的 定长的批次间隔） <br/>
> 支持在streaming过程中 重设 topics，用于生产中动态地增加删减数据源 <br/>
> 添加了速率控制，KafkaRateController。用来控制读取速率，由于不是用的sparkstreaming，所有速率控制的一些参数拿不到，得自己去计算。 <br/>
> 提供spark-streaming-kafka-0-10_2.10 spark 1.6 来支持 kafka的ssl <br/>
> 支持rdd.updateOffset 来管理偏移量。 <br/>
-------------------


## :jack_o_lantern: Table of contents
- [Spark kafka /sparkstreaming kafka](#Spark-kafka)
- [Spark Hbase](#spark-Hbase)
- [Spark ES Util](#Spark-ES-Util)
- [Spark Kudu](#Spark-Kudu)
- [Flink kafka](#Flink-kafka)
- [Kafka Util](#Kafka-Util)
- [Hbase Util](#Hbase-Util)
- [Database util](#database-util)
- [Elasticserach shade](#Elasticserach-shade)
- [Rabbitmq util](#Rabbitmq-util)
- [Splunk](#Splunk)

<a name="Spark-kafka"></a>
Spark kafka
------------
 - 封装了StreamingDynamicContext 。动态地调整 streaming的批次间隔时间，不像sparkstreaming的批次间隔时间是固定的（Streaming Kafka DynamicContext is encapsulated. Dynamically adjust the batch interval of streaming, unlike sparkstreaming, where the batch interval is fixed）
 - 使用StreamingDynamicContext 可以让你在流式程序的执行过程中动态的调整你的topic和获取kafkardd的方式。而不需要重新启动程序
 - 添加了 sparkStreaming 1.6 -> kafka 010  的 spark-streaming-kafka-0-10_2.10 。用以支持ssl 。
 - 封装了spark/sparkstreaming direct读取kafka数据的方式；提供rdd.updateOffset方法来手动管理偏移量到zk； 提供配置参数。<br>
 (Encapsulated spark/sparkstreaming to read Kafka with Low level integration (offset in zookeeper)。Provides many configuration parameters to control the way to read Kafka data)
 - 支持topic新增分区 <br>
 (Support topic to add new partition)
 - 支持rdd数据写入kafka 的算子 <br>
 (Supporting RDD data to write to Kafka)
 - 支持 Kafka SSL (提供spark 1.6 + Kafka 010 的整合api)（sparkstreaming 1.6 with kafka 010 ）  <br>
 (Support Kafka SSL (0.10+,spark 1.6+))
 - Add parameters ： 'kafka.consumer.from' To dynamically decide whether to get Kafka data from last or from consumption point
 - The version support of spark2.x Kafka 0.10+ is provided.（0.8, there is a big change compared to the 0.10 version.）
 - https://github.com/LinMingQiang/spark-util/tree/spark-kafka-0-8_1.6  或者  https://github.com/LinMingQiang/spark-kafka
 ```
   val kp = SparkKafkaContext.getKafkaParam(brokers,groupId,"consum","earliest") 
   val skc = new SparkKafkaContext(kp,sparkconf) 
   val kafkadataRdd = skc.kafkaRDD(topics,last,msgHandle)
   //...do something 
   kafkadataRdd.updateOffsets(groupId)//update offset to zk
 ```

<a name="spark-Hbase"></a>
Spark Hbase
------------
 * 根据scan条件扫描hbase数据成RDD  <br> 
 (spark scan hbase data to RDD) <br>
  scan -> RDD[T]
 * 根据RDD的数据来批量gethbase <br> 
 (spark RDD[T] get from hbase to RDD[U]) <br>
  RDD[T] -> Get -> RDD[U]
 * 根据RDD的数据来批量 写入  <br> 
 spark RDD[T] write to hbase <br>
  RDD[T] -> Put -> Hbase
 * 根据RDD的数据来批量更新rdd数据  <br>
  spark RDD[T] update with hbase data  <br>
  RDD[T] -> Get -> Combine -> RDD[U] <br>
 * 根据RDD的数据来批量更新rdd数据并写回hbase  <br> 
 spark RDD[T] update with hbase data then put return to hbase <br>
  RDD[T] -> Get -> Combine -> Put -> Hbase
 - https://github.com/LinMingQiang/spark-util/tree/spark-hbase
 ```
    val conf = new SparkConf().setMaster("local").setAppName("tets")
    val sc = new SparkContext(conf)
    val hc = new SparkHBaseContext(sc, zk)
    hc.hbaseRDD(tablename, f).foreach { println }
    hc.scanHbaseRDD(tablename, new Scan(), f)
```
 
<a name="Spark-ES-Util"></a>
Spark ES Util
------------
- spark集成es <br>
ElasticSearch integration for Apache Spark  <br>
- scan es数据为rdd  <br>
Scanning es data into RDD <br>
- https://github.com/LinMingQiang/spark-util/tree/spark-es
```
sc.esRDD("testindex/testtype", query)

```

<a name="Spark-Kudu"></a>
Spark Kudu
------------
- 读取kudu的数据为rdd  <br>
Read kudu data into RDD <br>
- 讲rdd数据写入kudu  <br>
Write RDD data to kudu <br>
- draw lessons from: https://github.com/tmalaska/SparkOnKudu
- https://github.com/LinMingQiang/spark-util/tree/spark-kudu

<a name="Flink-kafka"></a>
Flink kafka
------------
* 这是一个简单的例子。读取卡夫卡数据，实现WordCount统计并写入HBase<br>
* This is a simple example. Read Kafka data, implement WordCount statistics and write to HBase
- https://github.com/LinMingQiang/flink-demo

<a name="Splunk"></a>
Splunk
------------
* Splunk是一个日志显示和监视系统   <br>
(Splunk is a log display and monitoring system.)
* Splunk的安装和使用   <br>
(Installation and use of Splunk)
- https://github.com/LinMingQiang/spark-util/tree/splunk

<a name="Kafka-Util"></a>
Kafka Util
------------
*  操作kafka工具类，提供每天记录主题的偏移量，主要用于日重新计算、小时重新计算等功能。  <br>
Operate the tool class of kafka, provide offset to record topic by day, mainly used for day recalculation, hour recalculation and other functions  <br>
- https://github.com/LinMingQiang/spark-util/tree/kafka-util

<a name="Hbase-Util"></a>
Hbase Util
------------
* 操作Hbase的工具类，查询HBase表的region信息，用于手动分割过大的region <br>
The tool class that operates Hbase, inquires the region information of HBase table, used for manual split some excessive region  <br>
- https://github.com/LinMingQiang/spark-util/tree/hbase-util

<a name="Database-util"></a>
database util
------------
* Provides a connection tool for each database. include: es,hbase,mysql,mongo  <br>
- https://github.com/LinMingQiang/spark-util/tree/database-util

<a name="Elasticserach-shade"></a>
Elasticserach shade
------------
* Resolving conflicts between ES and spark and Hadoop related packages <br>
- https://github.com/LinMingQiang/spark-util/tree/es-shaed

<a name="Rabbitmq-util"></a>
Rabbitmq util
------------
* A tool class for sending and consuming MQ messages  <br>
https://github.com/LinMingQiang/spark-util/tree/rabbitmq-util

