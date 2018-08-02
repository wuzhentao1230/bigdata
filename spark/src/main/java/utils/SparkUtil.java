package utils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * 作者    吴振涛
 * 描述
 * 创建时间 2018年01月15日
 * 任务时间
 * 邮件时间
 */
public class SparkUtil {
   public static SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("taotao");

    //这个普遍是用sql曹操作  新特性包含了其他的context （hivecontext sparkcontext等）
    public static SparkSession sparkSession = SparkSession
            .builder()
            .config(sparkConf)
            .getOrCreate();
    //这个普遍进行Rdd操作的context
    public static JavaSparkContext context = new JavaSparkContext(sparkSession.sparkContext());
}
