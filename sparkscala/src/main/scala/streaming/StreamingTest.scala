package streaming

/** 作者    吴振涛
  * 描述    
  * 创建时间 2018年07月25日
  * 任务时间
  * 邮件时间
  */
import util.RedisPoolUtil
import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._

object StreamingTest {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    //获取sparkstreaming

    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")

    val ssc = new StreamingContext(conf, Seconds(10))

    val filter = new StopRecognition()

    RedisPoolUtil.initialPool();

    filter.insertStopNatures("w") //过滤掉标点
    filter.insertStopNatures("null") //过滤null词性
    filter.insertStopNatures("m"); //过滤m词性
    filter.insertStopWords("."); //过滤单词
    //    filter.insertStopRegexes("城.*?"); //支持正则表达式
    // Create direct kafka stream with brokers and topics
    val topics = "topicStream"
    val brokers = "192.168.21.3:9092"
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("bootstrap.servers" -> brokers,
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> "test-consumer-group")
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))


    // Split each line into words
    val lines = messages.map(_.value())

    val clearLines = lines.map { x =>
      val xx = ToAnalysis.parse(x).recognition(filter).toStringWithOutNature("\t")
      xx
    }

    val words = clearLines.flatMap(_.split("\t"))
    // Count each word in each batch
    val pairs = words.map(word => (word, 1))

    val wordCounts = pairs.reduceByKey(_ + _)
    // reduceByKeyAndWindow
    // 第二个参数，是窗口长度，这是是60秒
    // 第三个参数，是滑动间隔，这里是10秒
    // 也就是说，每隔10秒钟，将最近60秒的数据，作为一个窗口，进行内部的RDD的聚合，然后统一对一个RDD进行后续计算
    // 而是只是放在那里
    // 然后，等待我们的滑动间隔到了以后，10秒到了，会将之前60秒的RDD，因为一个batch间隔是5秒，所以之前60秒，就有12个RDD，给聚合起来，然后统一执行reduceByKey操作
    // 所以这里的reduceByKeyAndWindow，是针对每个窗口执行计算的，而不是针对 某个DStream中的RDD
    // 每隔10秒钟，出来 之前60秒的收集到的单词的统计次数
    val searchWordCountsDStream = wordCounts.reduceByKeyAndWindow((v1: Int, v2: Int) => v1 + v2, Seconds(60), Seconds(10))

    val finalDStream = searchWordCountsDStream.transform(searchWordCountsRDD => {
      val countSearchWordsRDD = searchWordCountsRDD.map(tuple => (tuple._2, tuple._1))
      val sortedCountSearchWordsRDD = countSearchWordsRDD.sortByKey(false)
      val sortedSearchWordCountsRDD = sortedCountSearchWordsRDD.map(tuple => (tuple._1, tuple._2))
      val top3SearchWordCounts = sortedSearchWordCountsRDD.take(5)
      val redis = RedisPoolUtil.getConn()
      var i = 0
      for (tuple <- top3SearchWordCounts) {
        i = i + 1
        redis.set(i + "", tuple._2 + ":" + tuple._1)
        println("result : " + tuple)
      }

      searchWordCountsRDD
    })
    finalDStream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}

