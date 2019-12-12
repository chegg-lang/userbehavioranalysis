package com.atguigu.netflowanalysis

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

case class UvCount(windowEnd: Long, count: Long)
/**
  * 自定义布隆过滤器实现在亿级用户访问的去重统计
  * 指标：网站独立访客数(uv)统计
  * 统计流量的重要指标时独立访客数:UniqueVisitor:指在一段时间内访问网站的总人数
  * 1天内同一访客的多次访问只记录为一个访客，通过IP和cookie一般是判断UV值得两种
  * 方式，通过自定义布隆过滤器实现亿级数据的去重过滤
  */

object UvWithBloomFilter {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream = env.readTextFile("D:\\workspaceIdea\\userbehavioranalysis\\NetworkFlowAnalysis\\src\\main\\resources\\UserBehavior.csv")
    .map(line => {
      val linearray = line.split(",")
      UserBehavior(linearray(0).toLong, linearray(1).toLong, linearray(2).toInt, linearray(3), linearray(4).toLong)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserBehavior](Time.milliseconds(0)) {
      override def extractTimestamp(t: UserBehavior) = t.timestamp * 1000L
    })
      .filter(_.behavior == "pv")
      .timeWindowAll(Time.hours(1))
      .trigger(new MyTrigger())
      .process(new UvCountWithBloomFilter())


    dataStream.print()
    dataStream.getSideOutput(new OutputTag[(String, String)]("duplicated")).print("duplicated")
    env.execute("Unique Visitor With Bloom Filter Job")
  }
}

class MyTrigger() extends Trigger[UserBehavior, TimeWindow] {
  //每条数据传入时所做的操作
  override def onElement(t: UserBehavior, l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext) = TriggerResult.FIRE_AND_PURGE

  //到达处理时间时所做的触发操作
  override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext) = TriggerResult.CONTINUE

  //当事件事件到达时，所做的触发操作
  override def onEventTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext) = TriggerResult.CONTINUE

  //清理在触发器中定义的资源呢
  override def clear(w: TimeWindow, triggerContext: Trigger.TriggerContext) = {}
}


//自定义布隆过滤器，大小(总bit数)外部传入，size必须是2的整次幂
class BloomFilter(size: Long) extends Serializable {
  private val cap = size

  //定义hash函数
  def hash(value: String, seed: Int) = {
    var result = 0L
    for (i <- value.indices) {
      result = result * seed + value.charAt(i) //自动类型转换，将字符类型转换成Int类型，Int类型可以自动转换成Long类型
    }
    //返回一个size范围内的hash值，按位与截掉高位
    (cap - 1) & result
  }
}

//自定义process function
class UvCountWithBloomFilter() extends ProcessAllWindowFunction[UserBehavior, UvCount, TimeWindow] {

  //定义redis连接
  private var jedis: Jedis = _
  private var bloom: BloomFilter = _

  //初始化连接操作
  override def open(parameters: Configuration): Unit = {
    jedis = new Jedis("hadoop102", 6379)
    //定义一个布隆过滤器，大小为2^29，5亿多位的bitmap，占2^26 byte，大概64M
    bloom = new BloomFilter(1 << 29)
  }

  override def process(context: Context, elements: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
    //在redis中存储位图，每个窗口用一个bitmap，所以就用窗口的end时间作为key
    val storeKey = context.window.getEnd.toString
    //把当前窗口的UV count值也存入redis，每次根据id是否重复进行读写，在redis里存成一个叫做count的hashMap
    var count = 0L
    //从redis中取出count值
    if (jedis.hget("count", storeKey) != null) {
      count = jedis.hget("count", storeKey).toLong
    }
    //判断userId是否出现过
    val userId = elements.last.userId.toString
    //按照userId计算在bitmap中的偏移量
    val offset: Long = bloom.hash(userId, 61)
    //到redis中查村bitmap，判断是否有位图上是否有该用户
    val isExists = jedis.getbit(storeKey, offset)

    if (!isExists) {
      //如果不存在，那么将对应位置上设置为1，并且更改count状态
      jedis.setbit(storeKey, offset, true)
      jedis.hset("count", storeKey, (count + 1).toString)
      //输出到流里
      out.collect(UvCount(storeKey.toLong,count + 1))
    }else{
      context.output(new OutputTag[(String,String)]("duplicated"),(userId,"duplicated"))
    }
  }
}













