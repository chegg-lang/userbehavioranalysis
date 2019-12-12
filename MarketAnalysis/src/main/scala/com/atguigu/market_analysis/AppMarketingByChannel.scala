package com.atguigu.market_analysis

import java.sql.Timestamp
import java.util.UUID

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random



/**
  * 需求:App市场推广统计
  *   对于电商企业来说，一般会通过各种不同的渠道对自己的App进行市场推广，而这些渠道
  *   的统计数据(比如，不同网站上广告链接的点击量、APP下载量)就成了市场营销的重要
  *   商业指标
  *   统计:分渠道市场推广统计
  */

//定义数据源样例类
case class MarketingUserBehavior(userId: String, behavior: String, channel: String, timestamp: Long)

// 输出分渠道统计的计数值样例类
case class MarketingViewCount( windowStart: String, windowEnd: String, channel: String, behavior: String, count: Long )

//自定义数据源，生成测试数据
class SimulatedEventSource extends RichParallelSourceFunction[MarketingUserBehavior]{
  // 定义运行的标识位
  var running = true
  // 定义行为和渠道的集合
  val channelSet: Seq[String] = Seq( "AppStore", "HuaweiStore", "XiaoStore", "tieba", "weibo", "wechat" )
  val behaviorTypes: Seq[String] = Seq( "INSTALL", "DOWNLOAD", "PURCHASE", "UNINSTALL" )
  // 定义随机数生成器
  val rand: Random = Random

  override def cancel(): Unit = running = false

  override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
    while(running){
      // 所有数据随机生成
      val id = UUID.randomUUID().toString
      val behavior = behaviorTypes(rand.nextInt(behaviorTypes.size))
      val channel = channelSet(rand.nextInt(channelSet.size))
      val ts = System.currentTimeMillis()

      ctx.collect(MarketingUserBehavior(id, behavior, channel, ts))
      Thread.sleep(10L)
    }
  }
}


object AppMarketingByChannel {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream: DataStream[MarketingViewCount] = env.addSource(new SimulatedEventSource)
      .assignAscendingTimestamps(_.timestamp)
      .filter(_.behavior != "UNINSTALL")
      .keyBy(_.channel)
      .timeWindow(Time.hours(1), Time.seconds(5))
      .aggregate(new CountAgg(), new WindowResultCount())


    dataStream.print()

    env.execute()



  }
}

// 自定义预聚合函数
class CountAgg() extends AggregateFunction[MarketingUserBehavior, Long, Long]{
  override def add(value: MarketingUserBehavior, accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}
// 自定义窗口函数
class WindowResultCount() extends WindowFunction[Long, MarketingViewCount, String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[MarketingViewCount]): Unit = {
    val start = new Timestamp(window.getStart).toString
    val end = new Timestamp(window.getEnd).toString
    out.collect( MarketingViewCount(start, end, key, "", input.iterator.next()) )
  }
}
