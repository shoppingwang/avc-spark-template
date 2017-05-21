package com.avcdata.data.template.spark.streaming.processor

import com.avcdata.etl.common.util.KafkaUtil
import com.avcdata.etl.streaming.template.StreamProcessTemplate
import com.avcdata.etl.streaming.util.KafkaStreamUtil
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}

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
    //需要消费的topic
    val topicsSet = configItems("kafka.read.topics").split(",").toSet

    //Kafka的ZK地址
    val zookeeperAddress = configItems("kafka.zookeeper.hosts")

    //topic消费信息
    val commonConnectUri = configItems("data.common.db.connecturi")
    val commonUsername = configItems("data.common.db.username")
    val commonPassword = configItems("data.common.db.password")

    val fromOffsets = KafkaUtil.fromOffsets(commonConnectUri, commonUsername, commonPassword, topicsSet, zookeeperAddress)

    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, String](
      "auto.offset.reset" -> configItems.getOrElse("kafka.auto.offset.reset", "largest"),
      "zookeeper.connect" -> zookeeperAddress,
      "group.id" -> "TimeHonoredBrandPosts",
      "metadata.broker.list" -> KafkaUtil.getBrokerList(zookeeperAddress)
    )

    //获取流数据
    val kafkaStream = fromOffsets.map
    { fo =>

      val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message)

      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fo, messageHandler)
    } getOrElse KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    //主处理逻辑
    kafkaStream.foreachRDD(jsonRDD =>
    {
      //获取当前消费数据的偏移量信息
      val offsetRanges = jsonRDD.asInstanceOf[HasOffsetRanges].offsetRanges
      logInfo(s"The current processing offset ranges:\n${offsetRanges.mkString("\n")}")

      //TODO 业务处理逻辑
      jsonRDD.foreach(println)

      //更新数据库topic的offset值
      KafkaStreamUtil.updateDBOffsets(commonConnectUri, commonUsername, commonPassword, offsetRanges)
    })
  }
}
