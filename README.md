# :tada:branch-1.6.0-0.10
---------------------
> - 解决了批次计算延迟后出现的任务append导致整体恢复后 计算消费还是跟不上的问题
> - 支持动态调节 streaming 的 批次间隔时间 （不同于sparkstreaming 的 定长的批次间隔，StructuredStreaming中使用trigger实现了。） <br/>
> - 支持在streaming过程中 重设 topics，用于生产中动态地增加删减数据源 <br/>
> - 添加了速率控制，KafkaRateController。用来控制读取速率，由于不是用的sparkstreaming，所有速率控制的一些参数拿不到，得自己去计算。
> - 提供spark-streaming-kafka-0-10_2.10 spark 1.6 来支持 kafka的ssl <br/>
> - 支持rdd.updateOffset 来管理偏移量。 <br/>
---------------------
# :tada: branch-sparkstreaming-1.6.0-0.10
---------------------
> - 只是结合了 sparkstreaming 1.6 和 kafka 010 。 使低版本的spark能够使用kafka的ssl验证 <br>
> - 支持 SSL
> - 支持spark 1.6 和 kafka 0.10 的结合
> - 支持管理offset
--------------------
# :tada:branch-2.0.1-0.10
-------------------
> - 解决了批次计算延迟后出现的任务append导致整体恢复后 计算消费还是跟不上的问题
> - 支持动态调节 streaming 的 批次间隔时间 （不同于sparkstreaming 的 定长的批次间隔，StructuredStreaming中使用trigger实现了。） <br/>
> - 支持在streaming过程中 重设 topics，用于生产中动态地增加删减数据源 <br/>
> - 提供spark-streaming-kafka-0-10_2.10 spark 1.6 来支持 kafka的ssl <br/>
> - 支持rdd.updateOffset 来管理偏移量。 <br/>
> - 由于kakfa-010 的api的变化，之前的 kafka-08 版本的 spark-kafka 虽然能用，但是他依赖于spark-streaming-kafka-0-8_2.10 <br/>.(可能会导致一些版本问题)；所以这次重新写了一个 kafka010 & spark-2.x 版本 ；但是使用方法还是跟之前的差不多， <br/>
> - kafka010有两种来管理offset的方式，一种是旧版的用zookeeper来管理，一种是本身自带的。现只提供zookeeper的管理方式
> - 要确保编译的kafka-client的版本和服务器端的版本一致，否则会报 Error reading string of length 27489, only 475 bytes available 等错误<br/>
> - 添加了速率控制，KafkaRateController。用来控制读取速率，由于不是用的sparkstreaming，所有速率控制的一些参数拿不到，得自己去计算。<br>
-------------------

# :tada:Spark-Util

## POINT

> -  spark与其他组件的封装api <br>
> -  支持动态调节 streaming 的 批次间隔时间 （不同于sparkstreaming 的 定长的批次间隔，在Structed Streaming中使用trigger触发机制实现）;不使用streamingContext 来实现流式计算，因为streamingContext是严格的时间间隔执行job任务，当job时间远小于batchtime时，会有大量的时间是在sleep等待下一个批次执行的到来(具体可以看看streamingContext的源码);StreamingDynamicContext 的设计借鉴了streamingContext的设计。但是在Job的提交上不使用Queue队列来appending堆积的job。当job执行完后，用户可以自己选择是否立刻执行下一个批次的计算，还是选择继续等待指定时长。<br>
> - 支持在streaming过程中 重设 topics，用于生产中动态地增加删减数据源 <br/>
> - 添加了速率控制，KafkaRateController。用来控制读取速率，由于不是用的sparkstreaming，所有速率控制的一些参数拿不到，得自己去计算。 <br/>
> - 提供spark-streaming-kafka-0-10_2.10 spark 1.6 来支持 kafka的ssl <br/>
> - 支持rdd.updateOffset 来管理偏移量。 <br/>
> - 封装 StreamingKafkaContext ：你依然可以用 streamingContext来实现流式计算，词Api封装了读取kafka数据。<br><br>


## Support
---
|                    | scala version      |Kafka version       | hbase 1.0+         | es   2.3.0         |kudu  1.3.0         |SSL         |
|:------------------:|:------------------:|:------------------:|:------------------:|:------------------:|:------------------:|:------------------:|
| **spark 1.3.x**    | 2.10               | 0.8               | :ok_hand: | :star2: | :eggplant: |NO |
| **spark 1.6.x**    | 2.10               | 0.8               | :baby_chick: | :santa: | :corn: |NO |
| **spark 1.6.x**    | 2.10               | 0.10+               | :baby_chick: | :santa: | :corn: |YES |
| **spark 2.0.x**    | 2.10/2.11          | 0.10+               | :smiley: | :cherries: | :peach: |YES |

---


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