package edu.dataart.kolhun.kafka

import java.util.{Calendar, Properties}

import edu.dataart.kolhun.kafka.dto.Device
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

import scala.util.Random

object DeviceProducer {


  def main(args: Array[String]) {
    val events = 1000
    val topic = "deviceTopic"
    val brokers = "localhost:9092"
    val props = new Properties()
    props.put("metadata.broker.list", brokers)
    props.put("serializer.class", "edu.dataart.kolhun.kafka.adapter.DeviceEncoder")
    props.put("producer.type", "async")
    props.put("queue.buffering.max.ms", "100")

    val config = new ProducerConfig(props)
    val producer = new Producer[String, Device](config)
    for (nEvents <- Range(0, events)) {
      val randomValue = BigDecimal(Random.nextDouble() * 100).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble
      val randomSuffix = Random.nextString(3)
      val device = new Device(timestamp = Calendar.getInstance().getTime(), device = "device" + randomSuffix, randomValue)
      val data = new KeyedMessage[String, Device](topic, device)
      Thread.sleep(1000)
      producer.send(data)
      println("produced" + System.currentTimeMillis() + " " + device.timestamp.toInstant + " " + device.device + " " + device.value)
    }
    producer.close()
  }

}