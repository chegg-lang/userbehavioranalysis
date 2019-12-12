package com.atguigu.netflowanalysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


/**
  * 以用户行为csv文件为数据源，统计pv(page view)
  * 统计思路:在一段时间内的页面访问量，直接进行sum即可
  */


//定义样例类，用于处理数据
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

object PageViewAnalysis {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream: DataStream[String] = env.readTextFile("D:\\workspaceIdea\\userbehavioranalysis" +
      "\\NetworkFlowAnalysis\\src\\main\\resources\\UserBehavior.csv")
    dataStream.map(record =>{
      var dataArr = record.split(",")
      UserBehavior(dataArr(0).toLong,dataArr(1).toLong,dataArr(2).toInt,dataArr(3),dataArr(4).toLong)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserBehavior](Time.milliseconds(100l)) {
      override def extractTimestamp(t: UserBehavior) = t.timestamp * 1000L
    })
      .filter(_.behavior == "pv")
      .map(data=>("pv",1))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .sum(1)
      .print()

    env.execute()
  }

}
