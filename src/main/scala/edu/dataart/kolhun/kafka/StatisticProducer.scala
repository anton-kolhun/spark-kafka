package edu.dataart.kolhun.kafka

import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}


class StatisticProducer(topic: String) {
  val events = 1000
  val brokers = "localhost:9092"
  val props = new Properties()
  props.put("metadata.broker.list", brokers)
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  props.put("producer.type", "async")
  props.put("queue.buffering.max.ms", "100")
  val config = new ProducerConfig(props)
  val producer = new Producer[String, String](config)


  def sendStringMessage(message: String) {
    val data = new KeyedMessage[String, String](topic, message)
    producer.send(data)
  }


}
