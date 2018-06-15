package learning;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import utils.SparkUtil;

import java.util.Arrays;

/**
 * 作者    吴振涛
 * 描述
 * 创建时间 2018年01月15日
 * 任务时间
 * 邮件时间
 */
public class DistinctExamples {
    public static void main(String[] args) {
        SparkConf sparkConf = SparkUtil.sparkConf;
        SparkSession spark = SparkUtil.sparkSession;
        JavaSparkContext context = SparkUtil.context;

        JavaRDD<Integer> lines = context.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 9, 7, 5, 3, 2, 2), 1);
        System.out.println(lines.collect());
        System.out.println("去重"+lines.distinct().collect());
    }
}
