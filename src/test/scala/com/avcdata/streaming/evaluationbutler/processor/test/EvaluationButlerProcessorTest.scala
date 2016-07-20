package com.avcdata.streaming.evaluationbutler.processor.test

import com.avcdata.etl.streaming.launcher.StreamProcessLauncher
import org.junit.Test

/**
  * 评价管家处理器测试
  * <p/>
  * Author   : wangxp
  * <p/>
  * DateTime : 16/7/11 13:30
  */
class EvaluationButlerProcessorTest
{
  @Test
  def testProcess(): Unit =
  {
    val args = Array[String]("--process-class"
      , "com.avcdata.etl.streaming.processor.EvaluationButlerProcessor"
      , "--batch-duration-seconds"
      , "10"
      , "--param"
      , "redis.host=127.0.0.1"
      , "--param"
      , "redis.port=6379"
      , "--param"
      , "redis.listen.keys=mylist"
      , "--param"
      , "es.index.auto.create=true"
      , "--param"
      , "es.resource=avcdata/evaluationbutler"
      , "--param"
      , "es.nodes=127.0.0.1"
      , "--param"
      , "es.port=9200"
      , "--param"
      , "es.input.json=false"
      , "--param"
      , "es.write.operation=upsert"
      , "--param"
      , "es.output.json=false"

//      , "--param"
//      , "es.nodes.discovery=false"
//      , "--param"
//      , "es.nodes.client.only=true"
    )

    StreamProcessLauncher.main(args)
  }
}
