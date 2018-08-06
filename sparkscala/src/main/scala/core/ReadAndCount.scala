package core

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author himanshu
 * Sample program to read a file in Spark and count the number of lines in the file
 *
 */
object ReadAndCountFile {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Group By Key").setMaster("local")
    val sc = new SparkContext(conf)

    //Read a text file
    val fileRDD = sc.textFile(args(0))

    //Count number of rows in an RDD and print
    println("Count in the file is " + fileRDD.count())

  }

}