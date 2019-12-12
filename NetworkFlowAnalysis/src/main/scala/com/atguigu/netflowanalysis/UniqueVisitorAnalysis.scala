package com.atguigu.netflowanalysis


import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


/**
  * 指标：网站独立访客数(uv)统计
  * 统计流量的重要指标时独立访客数:UniqueVisitor:指在一段时间内访问网站的总人数
  * 1天内同一访客的多次访问只记录为一个访客，通过IP和cookie一般是判断UV值得两种
  * 方式，涉及到去重的操作
  */
//case class UvCount(windowEnd: Long, count: Long)

object UniqueVisitorAnalysis {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream: DataStream[String] = env.readTextFile("D:\\workspaceIdea\\userbehavioranalysis\\NetworkFlowAnalysis\\src\\main\\resources\\UserBehavior.csv")
    dataStream.map(line => {
      val linearray = line.split(",")
      UserBehavior(linearray(0).toLong, linearray(1).toLong, linearray(2).toInt, linearray(3), linearray(4).toLong)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserBehavior](Time.milliseconds(0)) {
      override def extractTimestamp(t: UserBehavior) = t.timestamp *1000L
    })
      .filter(_.behavior == "pv")
      .timeWindowAll(Time.hours(1))
      .apply(new UvCountByWindow())
      .print()

    env.execute("Unique Visitor Job")
  }
}

class UvCountByWindow() extends AllWindowFunction[UserBehavior,UvCount,TimeWindow]{
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
    //将用户Id 放入到set集合中实现去重操作
    var idSet = scala.collection.mutable.Set[Long]()

    for (userBehavior <- input) {
      idSet += userBehavior.userId
    }
    out.collect(UvCount(window.getEnd,idSet.size))
  }
}