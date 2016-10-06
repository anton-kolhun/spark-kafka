package edu.dataart.kolhun.kafka

import java.util.Properties

import edu.dataart.kolhun.kafka.adapter.DeviceDecoder
import kafka.consumer.{Consumer, ConsumerConfig}


object DeviceConsumer {

  val ZOOKEEPER_ADDRESS = "localhost:2181"
  val GROUP_NAME = "group1"
  val TOPIC_NAME = "deviceTopic"

  val config = createConsumerConfig()
  val consumer = Consumer.create(config)
  val deviceDecoder = new DeviceDecoder()

  def createConsumerConfig(): ConsumerConfig = {
    val props = new Properties()
    props.put("zookeeper.connect", ZOOKEEPER_ADDRESS)
    props.put("group.id", GROUP_NAME)
    props.put("auto.offset.reset", "largest")
    props.put("zookeeper.session.timeout.ms", "400")
    props.put("zookeeper.sync.time.ms", "100")
    props.put("auto.commit.interval.ms", "100")
    val config = new ConsumerConfig(props)
    config
  }


  def main(args: Array[String]) {
    val topicCountMap = Map(TOPIC_NAME -> 1)
    val consumerMap = consumer.createMessageStreams(topicCountMap)
    val streams = consumerMap.get(TOPIC_NAME).get
    for (stream <- streams) {
      val it = stream.iterator()

      while (it.hasNext()) {
        val binary = it.next().message()
        val device = deviceDecoder.fromBytes(binary)
        println(System.currentTimeMillis() + " " + device.timestamp.toInstant +  " " + device.device + " " + device.value)
      }
    }
  }


}



