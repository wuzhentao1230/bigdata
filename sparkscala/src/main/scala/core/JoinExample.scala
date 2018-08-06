package core

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author himanshu
 * Example to join two files in Spark
 * *
 */
object JoinExample {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Join Example").setMaster("local")
    val sc = new SparkContext(conf)

    //Read first file into an RDD
    val inputNamesRDD = sc.textFile(args(0))
    //Read second file into an RDD
    val addressRDD = sc.textFile(args(1))



    //Convert into key value RDD
    val keyValueInputNamesRDD = inputNamesRDD.map { x => (x.split(",")) }.map { x => (x(0), x) }

    //Convert into key value RDD
    val keyValueAddressRDD = addressRDD.map { x => (x.split(",")) }.map { x => (x(0), x) }

    //Join two RDDs
    val joinRDD = keyValueInputNamesRDD.join(keyValueAddressRDD)

    //Print output of joined RDD
    joinRDD.map(f => f._1 + "|" + f._2._1(1) + "|" + f._2._1(2) + "|" + f._2._1(3) + "|" + f._2._2(1)).foreach { println }

  }
  


}