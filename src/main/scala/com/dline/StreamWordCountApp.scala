package com.dline

import org.apache.flink.streaming.api.scala._

/**
 * 测试方法：打开控制台，执行：
 *   nc -lk 7777
 */
object StreamWordCountApp {

  def main(args: Array[String]): Unit = {

    //创建流处理环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //接收一个socket文本流
    val dataStream: DataStream[String] = env.socketTextStream("", 7777)

    var wordCountDataStream = dataStream.flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map( (_, 1))
      .keyBy(0)
      .sum(1)

    wordCountDataStream.print()

    // 开始执行
    env.execute("stream word count job")
  }
}
