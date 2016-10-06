package edu.dataart.kolhun.kafka.adapter

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import edu.dataart.kolhun.kafka.dto.Device
import kafka.serializer.Encoder
import kafka.utils.VerifiableProperties


class DeviceEncoder(props: VerifiableProperties = null) extends Encoder[Device] {

  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  val encoding =
    if (props == null)
      "UTF8"
    else
      props.getString("serializer.encoding", "UTF8")

  override def toBytes(device: Device): Array[Byte] =
    if (device == null)
      null
    else
      mapper.writeValueAsString(device).getBytes(encoding)
}
