package core

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/** 作者    吴振涛
  * 描述    
  * 创建时间 2018年08月03日
  * 任务时间
  * 邮件时间
  */
object RDDLearning {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[4]").setAppName("hello RDD")
    val sc = new SparkContext(conf)
    sc.makeRDD(Array(1, 2, 3)).foreach(println)

    val rdd = sc.parallelize(List("coffee panda","happy panda","happiest panda party"))
    val mapRDD=rdd.map{x=>x.split(" ")}
    mapRDD.foreach(println)


    //flatmap是将很对对象切碎后合成一个大的对象
    val flatmapRDD=rdd.flatMap{x=>x.split(" ")}
    flatmapRDD.foreach(println)
    sc.stop()

  }
}
