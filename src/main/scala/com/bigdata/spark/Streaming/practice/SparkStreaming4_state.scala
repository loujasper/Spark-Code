package com.bigdata.spark.Streaming.practice

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}


object SparkStreaming4_state {

  def main(args: Array[String]): Unit = {

//    SparkStreaming最少要两个核
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Streaming")

    val ssc = new StreamingContext(sparkConf, Seconds(3))

    ssc.sparkContext.setCheckpointDir("cp")   //里计算的中间结果需要报错到检查点的位置中。所以需要设定检查点路径。

//    从socket拿数据
    val ds: ReceiverInputDStream[String] = ssc.socketTextStream("localhost",9999)

//    TODO 装换，
//    有状态的保存，
//    将spark每个采集周的数据的处理结果保存起来,然后后续的数据进行聚合操作.
//    有状态操作的目的就是每一个采集周期的数据的计算记过临时保存起来，在下一次数据处理当中可以继续使用
    /**
      * updateStateByKey()
      * 第一个参数seq，表示相同key的value的集合
      * 第二个参数Buffer:表示相同key的缓冲区的集合
      *
      * 报错：requirement failed: The checkpoint directory has not been set.
      * 这里计算的中间结果需要报错到检查点的位置中。所以需要设定检查点路径。
      */
    ds
      .flatMap(_.split(" "))
      .map((_,1L))
//      .reduceByKey(_+_)  //reduceByKey()是无状态的，需要有状态的数据操作
      .updateStateByKey[Long](
      (seq:Seq[Long], buffer: Option[Long]) => { //option用来临时保存起来，类似缓冲区  seq:满足条件相同key的value的集合
        val newBufferValue = buffer.getOrElse(0L) + seq.sum  // 将当前的状态更新到缓冲区中
        Option(newBufferValue)
      }
    )  //根据key更新状态
      .print()


//    TODO 启动采集器
    ssc.start()

//    TODO 等待采集器结束
    ssc.awaitTermination()






  }
}
