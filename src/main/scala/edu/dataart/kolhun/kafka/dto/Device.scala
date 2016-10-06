package edu.dataart.kolhun.kafka.dto

import java.util.Date


class Device(val timestamp: Date,
             val device: String,
             val value: Double) extends Serializable {
}


