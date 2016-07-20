package com.avcdata.streaming.evaluationbutler.processor

import com.avcdata.streaming.evaluationbutler.pojo.EvaluationButlerDetails
import com.avcdata.etl.streaming.template.StreamProcessTemplate
import com.redislabs.provider.redis.streaming.RedisInputDStream
import org.apache.commons.lang.exception.ExceptionUtils
import org.apache.hadoop.hbase.client.{Connection, Put}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.elasticsearch.spark.rdd.EsSpark

import scala.util.Try

/**
  * 评价管家数据处理器
  * <p/>
  * Author   : wangxp
  * <p/>
  * DateTime : 16/7/6 13:51
  */
class EvaluationButlerProcessor(batchDurationSeconds: Int, otherConfigrations: Map[String, String])
  extends StreamProcessTemplate(batchDurationSeconds: Int, otherConfigrations: Map[String, String])
{
  /**
    * 使用ssc进行业务逻辑处理
    */
  protected override def process(ssc: StreamingContext): Unit =
  {
    import com.redislabs.provider.redis._
    import org.apache.spark.storage.StorageLevel

    val listenKeys = ssc.sparkContext.getConf.get("redis.listen.keys").split(",").map(_.trim)
    val redisStream = ssc.createRedisStreamWithoutListname(listenKeys, storageLevel = StorageLevel.MEMORY_AND_DISK_2)

    //数据处理
    val handledEvaluationInfos = handleData(redisStream)

    //创建HBaseContext对象
    val hbaseContext = new HBaseContext(ssc.sparkContext, HBaseConfiguration.create())

    //数据入库
    handledEvaluationInfos.foreachRDD(handledEvaluationInfoRDD =>
    {
      //缓存计算结果
      handledEvaluationInfoRDD.cache()

      saveToHBase(hbaseContext, handledEvaluationInfoRDD)

      saveToES(handledEvaluationInfoRDD)

      //去除缓存计算结果
      handledEvaluationInfoRDD.unpersist()
    })

    ssc.start()

    ssc.awaitTermination()
  }


  /**
    * 处理数据
    *
    * @param redisStream 待处理数据流
    * @return 处理后的数据流
    */
  private def handleData(redisStream: RedisInputDStream[String]): DStream[EvaluationButlerDetails] =
  {
    redisStream.map(evaluationJson =>
    {
      Try
      {
        try
        {
          import net.liftweb.json._
          implicit val formats = DefaultFormats

          parse(evaluationJson).extract[EvaluationButlerDetails]
        }
        catch
        {
          case ex: Throwable => println(s"Handle data >$evaluationJson< fail\n", ExceptionUtils.getFullStackTrace(ex)); throw ex
        }
      }
    }).filter(_.isSuccess).map(_.get)
  }


  /**
    * 保存数据至ES
    *
    * @param handledEvaluationInfoRDD 需要保存的数据
    */
  private def saveToES(handledEvaluationInfoRDD: RDD[EvaluationButlerDetails]): Unit =
  {
    //import org.elasticsearch.spark._
    //handledEvaluationInfoRDD.saveToEs(otherConfigrations("es.resource"), Map("es.mapping.id" -> "brand", "es.nodes" -> "192.168.100.200:9200"))
    EsSpark.saveToEs(handledEvaluationInfoRDD.map(handledEvaluationInfo =>
    {
      Map("brand" -> handledEvaluationInfo.brand, "platform" -> handledEvaluationInfo.platform, "category" -> handledEvaluationInfo.category)
    }), otherConfigrations("es.resource"), Map("es.mapping.id" -> "brand"))
  }


  /**
    * 保存数据至HBase
    *
    * @param hbaseContext             HBaseContext
    * @param handledEvaluationInfoRDD 需要保存的RDD
    */
  private def saveToHBase(hbaseContext: HBaseContext, handledEvaluationInfoRDD: RDD[EvaluationButlerDetails]): Unit =
  {
    //对每个RDD执行入HBASE操作
    hbaseContext.foreachPartition(handledEvaluationInfoRDD, (it: Iterator[EvaluationButlerDetails], conn: Connection) =>
    {
      //HBSE表操作对象
      val bufferedMutator = conn.getBufferedMutator(TableName.valueOf("evaluation_bulter"))

      it.foreach(handledEvaluationInfo =>
      {
        val put = new Put(Bytes.toBytes(handledEvaluationInfo.brand))
        put.addColumn(Bytes.toBytes("meta"), Bytes.toBytes("platform"), Bytes.toBytes(handledEvaluationInfo.platform))
        put.addColumn(Bytes.toBytes("meta"), Bytes.toBytes("brand"), Bytes.toBytes(handledEvaluationInfo.brand))
        put.addColumn(Bytes.toBytes("meta"), Bytes.toBytes("category"), Bytes.toBytes(handledEvaluationInfo.category))

        bufferedMutator.mutate(put)
      })

      bufferedMutator.flush()
      bufferedMutator.close()
    })
  }
}
