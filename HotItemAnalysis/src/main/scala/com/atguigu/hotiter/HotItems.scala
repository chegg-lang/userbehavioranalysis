package com.atguigu.hotiter

import java.sql.Timestamp
import java.util
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.mutable.ListBuffer

/**
  * 实现热门商品统计，某一个商品被点击多少次，然后将点击次数前几的商品进行输出
  * 实时统计热门top N商品
  *
  * @param userid
  * @param item
  * @param categoryId
  * @param behavior
  * @param timestamp
  */
//输入数据的样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

//中间聚合结果样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)


object HotItems {

  def main(args: Array[String]): Unit = {


    //创建执行环境

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

//   val properties = new Properties()
//
//    properties.setProperty("bootstrap.servers","hadoop102:9092")
//    properties.setProperty("group.id","consumer-group")
//    properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
//    properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
//    properties.setProperty("auto.offset.reset","latest")
//
//    //kafka读取数据
//    val dataStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("hotitems",new SimpleStringSchema(),properties))

       val dataStream: DataStream[String] = env.readTextFile(
         "D:\\workspaceIdea\\userbehavioranalysis\\HotItemAnalysis\\src\\main\\resources\\UserBehavior.csv")
//    val dataStream = env.socketTextStream("hadoop102",7777)
    //指定事件时间语义
    env.setParallelism(1)

    //从文件当中读取数据，进行预处理，转换类型，封装样例类，分配时间戳越接近source越好
    val userBehaviorStream: DataStream[UserBehavior] = dataStream.map(record => {
      val arr: Array[String] = record.split(",")
      UserBehavior(arr(0).trim.toLong, arr(1).trim.toLong, arr(2).trim.toInt, arr(3).trim, arr(4).trim.toLong)
    }
    ).assignAscendingTimestamps(_.timestamp * 1000L)

    //分组聚合，分组排序输出，先过滤，过滤不应该放在时间语义之前，以免有的事件时间过长，导致窗口关不掉
    val aggStream: DataStream[ItemViewCount] = userBehaviorStream.filter(_.behavior == "pv") //过滤，只考虑pv类型的数据
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1), Time.minutes(5)) //定义滑窗
      .aggregate(new CountAgg(), new WindowCountResult())

    val resultDataStream = aggStream
      .keyBy(_.windowEnd)
      .process(new TopNhotTtems(3))

    resultDataStream.print("result")
    aggStream.print("agg")

    env.execute("hot items job")
  }

}

//自定义的预聚合函数，计数器功能
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: UserBehavior, acc: Long): Long = acc + 1L

  override def getResult(acc: Long): Long = acc

  //不同分区的数据进行聚合，shuffle分区操作，状态的merge
  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}


//自定义窗口函数，输出ItemViewCount
class WindowCountResult() extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    out.collect(ItemViewCount(key, window.getEnd, input.iterator.next()))
  }
}

//自定义processFunction
class TopNhotTtems(topSize: Int) extends KeyedProcessFunction[Long, ItemViewCount, String]() {

  //先定义一个列表状态，用于保存所有的数据
  private var itemListState: ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    itemListState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("itemList-state", classOf[ItemViewCount]))
  }

  override def processElement(value: ItemViewCount, cxt: KeyedProcessFunction
    [Long, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
    //每条数据都存入ListState
    itemListState.add(value)
    //注册定时器
    cxt.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //为方便排序，获取所有的状态数据，放入一个List中
    var allItemCounts: ListBuffer[ItemViewCount] = ListBuffer()

    import scala.collection.JavaConversions._
    for (item <- itemListState.get()) {
      allItemCounts += item
    }
    itemListState.clear()

    //按照count值大小排序，输出结果
    val sortedItemCounts: ListBuffer[ItemViewCount] = allItemCounts.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

    //排名信息格式化打印输出
    val result: StringBuffer = new StringBuffer()

    result.append("=========================================\n")
    result.append("窗口关闭时间：").append(new Timestamp(timestamp - 1)).append("\n")

    //每个商品信息输出
    for (i <- sortedItemCounts.indices) {
      val currentItem: ItemViewCount = sortedItemCounts(i)

      result.append("NO").append(i+1).append(": ")
        .append(" 商品ID=").append(currentItem.itemId)
        .append(" 浏览量=").append(currentItem.count).append("\n")
    }
    result.append("=========================================\n")
    Thread.sleep(1000)
    out.collect(result.toString)
  }
}