package com.avcdata.data.template.spark.streaming.processor

import com.avcdata.etl.streaming.template.StreamProcessTemplate
import com.avcdata.etl.streaming.util.KafkaStreamUtil
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.HasOffsetRanges

/**
  * 示例工程处理器
  * <p/>
  * Author   : wangxp
  * <p/>
  * DateTime : 16/7/6 13:51
  */
class SampleProjectProcessor(batchDurationSeconds: Int, configItems: Map[String, String])
  extends StreamProcessTemplate(batchDurationSeconds: Int, configItems: Map[String, String])
{
  /**
    * 使用ssc进行业务逻辑处理
    */
  protected override def process(ssc: StreamingContext): Unit =
  {
    //topic消费信息
    val commonConnectURI = configItems("data.common.db.connecturi")
    val commonUsername = configItems("data.common.db.username")
    val commonPassword = configItems("data.common.db.password")

    //主处理逻辑
    KafkaStreamUtil.createDirectStream(ssc, configItems("kafka.read.topics"), configItems("kafka.zookeeper.hosts")
      , commonConnectURI, commonUsername, commonPassword, Map("fetch.message.max.bytes" -> String.valueOf(1024 * 1024 * 10))
    ).foreachRDD(jsonRDD =>
    {
      //获取当前消费数据的偏移量信息
      val offsetRanges = jsonRDD.asInstanceOf[HasOffsetRanges].offsetRanges
      logInfo(s"The current processing offset ranges:\n${offsetRanges.mkString("\n")}")

      //TODO 业务处理逻辑
      jsonRDD.foreach(println)

      //更新数据库topic的offset值
      KafkaStreamUtil.updateDBOffsets(commonConnectURI, commonUsername, commonPassword, offsetRanges)
    })
  }
}
