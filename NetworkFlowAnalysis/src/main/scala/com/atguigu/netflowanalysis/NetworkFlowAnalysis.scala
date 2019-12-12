package com.atguigu.netflowanalysis


import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util
import java.util.{Map, Properties}

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, MapState, MapStateDescriptor}

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


//定义要处理数据的样例类ApacheLogEvent
case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)

//定义窗口统计函数输出数据的类型
case class UrlViewCount(url: String, windowEnd: Long, count: Long)

/**
  * 实时流量统计
  * 实现：每个5秒，输出最近10分钟内访问量最多的前N各URL。
  */
object NetworkFlowAnalysis {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置时间语义为时间时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

       val properties = new Properties()
        properties.setProperty("bootstrap.servers","hadoop102:9092")
        properties.setProperty("group.id","consumer-group1")
        properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty("auto.offset.reset","earliest")

        //kafka读取数据
        val dataStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("urlTopN",new SimpleStringSchema(),properties))

//    val dataStream: DataStream[String] = env.readTextFile(
//      "NetworkFlowAnalysis/src/main/resources/apache.log")

    dataStream.map { record =>

      val dataArr: Array[String] = record.split(" ")
      //定义类转换源数据中时间戳格式
      val format = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
      val timestamp = format.parse(dataArr(3)).getTime
      ApacheLogEvent(dataArr(0).trim, dataArr(2).trim, timestamp, dataArr(5).trim, dataArr(6).trim)
    }.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.milliseconds(1000L)) {
      override def extractTimestamp(t: ApacheLogEvent) = t.eventTime
    }).filter(data =>{val pattern = "^((?!\\\\.(css|js)$).)*$".r           //过滤其他的访问url
      (pattern findFirstIn data.url).nonEmpty
    }).keyBy(_.url)                                                        //以url进行分组统计，各页面的访 问量
      .timeWindow(Time.minutes(10),Time.seconds(5))
      .aggregate(new CountAgg(),new WindowResultFunction())
      .keyBy(_.windowEnd)
      .process(new TopNHotUrls(5))
      .print()

    env.execute("Network Flow Job")


  }
}

//预聚合函数
class CountAgg() extends AggregateFunction[ApacheLogEvent,Long,Long]{
  override def createAccumulator() = 0L

  override def add(in: ApacheLogEvent, acc: Long) = acc + 1l

  override def getResult(acc: Long) = acc

  override def merge(acc: Long, acc1: Long) = acc + acc1
}

//窗口函数
class WindowResultFunction() extends WindowFunction[Long,UrlViewCount,String,TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    val count: Long = input.iterator.next()
    out.collect(UrlViewCount(key,window.getEnd,count))
  }
}

class TopNHotUrls(topSize: Int) extends KeyedProcessFunction[Long,UrlViewCount,String]{

  //定义状态MapState,用来保存每个窗口中的聚合类到状态，当定时器执行时获取所有的商品聚合值，进行操作
  lazy private val urlMapState: MapState[String, Long] = getRuntimeContext.getMapState(new MapStateDescriptor[String,Long]("urlMap-state",classOf[String],classOf[Long]))


  override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction
    [Long, UrlViewCount, String]#Context, collector: Collector[String]) = {
    //对每一条数据进行操作，放入到状态map集合中
    urlMapState.put(value.url,value.count)
    //同时注册定时器，定义状态，保存定时器，判断每条数据输入时有没有定时器，如果有，不做处理，如果没有创建定时器
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  //定义定时器，在定时器触发时执行逻辑
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction
    [Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //定义数据容器
    val allUrlViewCounts:ListBuffer[(String,Long)] = ListBuffer()
    //获取在状态列表中的所有聚合后的数据
    val iter: util.Iterator[Map.Entry[String, Long]] = urlMapState.entries().iterator()
    while (iter.hasNext){
      val entry: Map.Entry[String, Long] = iter.next()
      allUrlViewCounts += ((entry.getKey,entry.getValue))
    }
    urlMapState.clear()

    val sortedUrlViewCounts: ListBuffer[(String, Long)] = allUrlViewCounts.sortBy(_._2)(Ordering.Long.reverse).take(topSize)

    //每一个窗口调用一次，输出该窗口中聚合的前三的浏览量的访问url
    val result: StringBuilder = new StringBuilder

    result.append("=======================================\n")
    result.append("时间：").append(new Timestamp(timestamp -1 )).append("\n")
    for (i <- sortedUrlViewCounts.indices) {
      val currentUrlView: (String, Long) = sortedUrlViewCounts(i)
      result.append("No").append(i+1).append(":")
        .append("  URL=").append(currentUrlView._1)
        .append("  流量").append(currentUrlView._2).append("\n")
    }
    result.append("======================================\n")
    Thread.sleep(1000)
    //将处理完的数据发出
    out.collect(result.toString())
  }
}
