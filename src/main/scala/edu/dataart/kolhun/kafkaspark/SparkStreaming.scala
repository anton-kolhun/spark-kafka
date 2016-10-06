package edu.dataart.kolhun.kafkaspark

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import edu.dataart.kolhun.kafka.StatisticProducer
import edu.dataart.kolhun.kafka.adapter.DeviceDecoder
import edu.dataart.kolhun.kafka.dto.{Device, DeviceStatistic}
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

object SparkStreaming {

  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  val statisticProducer = new StatisticProducer("statisticTopic")

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName(getClass.getSimpleName)
      .set("spark.master", "local")
    val ssc = new StreamingContext(sparkConf, Seconds(60))
    val topicsSet = Set("deviceTopic")
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "localhost:9092")
    val messages = KafkaUtils.createDirectStream[String, Device, StringDecoder, DeviceDecoder](
      ssc, kafkaParams, topicsSet)

    val devices = messages.map(_._2)
    devices.foreachRDD(calculateDeviceStatistic(_))

    ssc.start()
    ssc.awaitTermination()
  }

  def calculateDeviceStatistic(rdd: RDD[Device]) {
    val sortedDevices = rdd
      .sortBy(_.value)
    val minDevice = sortedDevices.first()
    val maxDevice = sortedDevices.collect().last
    val totalValue = sortedDevices
      .map(_.value)
      .reduce(_ + _)
    val avgValue = totalValue / sortedDevices.count()

    val devStatistic = new DeviceStatistic(min = minDevice.value, max = maxDevice.value, avg = avgValue)
    statisticProducer.sendStringMessage(mapper.writeValueAsString(devStatistic))

    println(mapper.writeValueAsString(devStatistic))
  }

}
