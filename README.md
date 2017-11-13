# spark-kafka
scala version 2.10
spark version 1.3.0 1.6.0
kafka version 0.8

* 提供 对外的 ssc创建Dstream的方法。
* 提供 对外的 ssc利用conf创建Dstream的方法
* 提供 使用direct方式读取kafka数据的方法
* 提供 "kafka.consumer.from" -> "last"/"consum" 参数，来动态决定获取kafka数据是从last还是从消费点开始
* 提供 "wrong.groupid.from"->"earliest/last" 参数 ，决定新group id 或者过期的 group id 从哪开始读取
* 提供 fromoffset 参数，决定从具体的offset开始获取数据
* 提供 rdd的更新kafka offsets到zookeeper的方法(需要 import org.apache.spark.func.tool._)
* 提供 rdd 写数据进kakfa方法
* 提供StreamingKafkaContext，SparkKafkaContext使用更方便
* 提供KafkaDataRDD，封装了更新offset等操作在里面。不用再用隐式转换来添加这些功能了
* 提供一个kafkaCluster。可以用来单独获取kafka信息，如最新偏移量等信息
* 修改 增加updateOffsets方法不用提供group id
* 增加 更新偏移量至最新操作。updataOffsetToLastest
* 修改，在kp里面设置spark.streaming.kafka.maxRatePerPartition。这样可以针对不同的topic使用不同的limit

* 增加 kafka.last.consum 的参数 ：CUSTOM （使可以自定义offset） 在配置文件里增加  kafka.offset= ${offset}
  offset 格式为  topic,partition,offset|topic,partition,offset|topic,partition,offset
  (当然你要自己定义获取方式也可以。这个在SparkKafkaContext已经提供了相应的方法。需要你传入 fromoffset)
  
# Example StreamingKafkaContextTest
> StreamingKafkaContextTest 流式 
```
var kp = Map[String, String](
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder",
      "group.id" -> "group.id",
      "kafka.last.consum" -> "last")//决定是从哪里开始 
val topics = Set("test")
val ds = ssc.createDirectStream[(String, String)](kp, topics, fromoffset, msgHandle)
ds.foreachRDD { rdd => 
rdd.foreach(println)
rdd.updateOffsets(kp, "group.id")//更新zookeeper的offsets
}
ssc.start()
ssc.awaitTermination()
```
# Example StreamingKafkaContextTest With Confguration
> StreamingKafkaContextTest （配置文件，便于管理。适用于项目开发）
```
 val conf = new ConfigurationTest()
    initConf("conf/config.properties", conf)
    initJobConf(conf)
    println(conf.getKV())
    val scf = new SparkConf().setMaster("local[2]").setAppName("Test")
    val sc = new SparkContext(scf)
    val ssc = new StreamingContext(sc, Seconds(5))
    val ds = ssc.createDirectStream(conf, null, msgHandle)
    ds.foreachRDD { rdd => rdd.foreach(println) }
    ssc.start()
    ssc.awaitTermination()
 def initJobConf(conf:Configuration)={
    var kp = Map[String, String](
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder",
      "group.id" -> "group.id",
      "kafka.last.consum" -> "last")
    val topics = Set("test")
    conf.setKafkaParams(kp)
    conf.setTopics(topics)
  }

```
# Example SparkKafkaContext 
> SparkKafkaContext （适用于离线读取kafka数据）
```
val skc = new SparkKafkaContext(
      new SparkConf().setMaster("local").setAppName("SparkKafkaContextTest"))
    val kp =Map[String, String](
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder",
      "group.id" -> "group.id",
      "kafka.last.consum" -> "last")
    val topics = Set("test")
    val kafkadataRdd = skc.kafkaRDD[String, String, StringDecoder, StringDecoder, (String, String)](kp, topics, msgHandle)
    kafkadataRdd.foreach(println)
    kafkadataRdd.updateOffsets(kp)//更新kafka偏移量
    
```
# Example KafkaWriter 
> KafkaWriter （将rdd数据写入kafka）
```
 val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("Test"))
    val ssc = new StreamingKafkaContext(sc, Seconds(5))
    var kp = Map[String, String](
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder",
      "group.id" -> "test",
      "kafka.last.consum" -> "consum")
    val topics = Set("test")
    val ds = ssc.createDirectStream[(String, String)](kp, topics, msgHandle)
    ds.foreachRDD { rdd => 
      rdd.foreach(println)
      rdd.map(_._2)
         .writeToKafka(producerConfig, transformFunc(outTopic,_))
      }

    ssc.start()
    ssc.awaitTermination()
    
```
