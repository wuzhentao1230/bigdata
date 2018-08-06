package core

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author himanshu
 * Read a file. Group by ID and get count of objects by ID.
 * Write results into a file
 *
 */
object GroupBy {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]) {
    //  val property = new PropertiesHelper(args, 2);

    val filePath = "D:\\个人项目\\before\\bigdata\\sparkscala\\src\\main\\resources\\InputFile.txt"

    val outputPath = "target/outputpath/";

    val conf = new SparkConf().setAppName("Group By Example").setMaster("local")

    val sc = new SparkContext(conf)

    //Read a text file
    val fileRDD = sc.textFile(filePath)

    fileRDD.foreach(x => println(x(0)))
    //Modify fileRDD to a key value RDD
    val keyValueRDD = fileRDD.map { x => (x.split("|")) }.map { x => (x(0), 1) }
    //Coung number of occurences by ID
    val groupByRDD = keyValueRDD.reduceByKey((x, y) => (x + y))
    println(groupByRDD.getClass.getName)
    groupByRDD.foreach(println)

    //Change the format as required and write to an output file

    groupByRDD.map(f => f._1 + "|" + f._2).foreach(println)

  }

}