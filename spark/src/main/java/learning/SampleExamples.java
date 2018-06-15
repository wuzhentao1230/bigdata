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
public class SampleExamples {
    public static void main(String[] args) {
        SparkConf sparkConf = SparkUtil.sparkConf;
        SparkSession spark = SparkUtil.sparkSession;
        JavaSparkContext context = SparkUtil.context;

        JavaRDD<Integer> lines = context.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9), 1);
        System.out.println(lines.collect());
//        第一个参数withReplacement代表是否进行替换，如果选true，上面的例子中，会出现重复的数据
//        第二个参数fraction 表示随机的比例
//        第三个参数seed 表示随机的种子 随机子一样随机的结果一样  随机子不一样结果有大几率不一样
        JavaRDD<Integer> resultlines = lines.sample(false, 0.5, 1);
        System.out.println(resultlines.collect());
    }
}
