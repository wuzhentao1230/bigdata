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
public class CoalesceAndRepartitionExamples {
    public static void main(String[] args) {
        SparkConf sparkConf = SparkUtil.sparkConf;
        SparkSession spark = SparkUtil.sparkSession;
        JavaSparkContext context= SparkUtil.context;
//        coalesce() 可以将 parent RDD 的 partition 个数进行调整，比如从 5 个减少到 3 个，或者从 5 个增加到 10 个。
//        需要注意的是当 shuffle = false 的时候，是不能增加 partition 个数的（即不能从 5 个变为 10 个）。
        JavaRDD<Integer> lines = context.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9),3);
        System.out.println("原始分区"+lines.partitions().size());
        JavaRDD<Integer> newLines = lines.coalesce(2, true);
        System.out.println("后来分区"+newLines.partitions().size());
        JavaRDD<Integer> newLines1 = lines.repartition(6);
        System.out.println("后来分区"+newLines1.partitions().size());
    }
}
