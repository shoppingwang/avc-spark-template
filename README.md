#AVC Spark模板工程

## Streaming
继承下面任意一个类(普通流模板/容错流模板),覆写process(ssc: StreamingContext)方法实现业务逻辑处理
- com.avcdata.etl.streaming.template.StreamProcessTemplate
- FaultToleranceStreamProcessTemplate

### 使用方式

> spark-submit --master spark://nn.avcdata.com:7077 --name EvaluationButlerProcessor --driver-memory 2G --executor-memory 4G --driver-cores 2 --executor-cores 2 --num-executors 3 --total-executor-cores 24 --verbose --driver-java-options "-Dlog4j.configuration=classpath:log4j-debug.properties" --class com.avcdata.etl.streaming.launcher.StreamProcessLauncher etl-streaming-0.0.1-jar-with-dependencies.jar --process-class com.avcdata.etl.streaming.processor.EvaluationButlerProcessor --batch-duration-seconds 10 --param redis.host=192.168.100.202 --param redis.port=16379 --param redis.listen.keys=mylist --param es.index.auto.create=true --param es.resource=evaluation/butler --param es.nodes=192.168.100.200 --param es.port=9200 --param es.input.json=false --param es.write.operation=upsert --param es.output.json=false