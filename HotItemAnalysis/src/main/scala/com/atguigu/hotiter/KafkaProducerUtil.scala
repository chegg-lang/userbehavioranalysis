package com.atguigu.hotiter

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.BufferedSource

object KafkaProducerUitl {



  def main(args: Array[String]): Unit = {

    writeToKafka("hotitems")
  }

  def writeToKafka(topic : String) = {

    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers","hadoop102:9092")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer: KafkaProducer[String, String] = new KafkaProducer[String,String](properties)

    val bufferedSource: BufferedSource = io.Source.fromFile("D:\\workspaceIdea\\userbehavioranalysis\\HotItemAnalysis\\src\\main\\resources\\UserBehavior.csv")

    for (line <- bufferedSource.getLines()) {
      val record = new ProducerRecord[String,String](topic,line)
      producer.send(record)

    }


  }

}
