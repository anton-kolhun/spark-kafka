package edu.dataart.kolhun.kafka.adapter

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import edu.dataart.kolhun.kafka.dto.Device
import kafka.serializer.Decoder
import kafka.utils.VerifiableProperties


class DeviceDecoder(props: VerifiableProperties = null)  extends Decoder[Device] {

  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  def fromBytes(binary: Array[Byte]): Device = {
    mapper.readValue(binary, classOf[Device])
  }


}
