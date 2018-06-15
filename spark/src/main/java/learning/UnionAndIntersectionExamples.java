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
public class UnionAndIntersectionExamples {
    public static void main(String[] args) {
        SparkConf sparkConf = SparkUtil.sparkConf;
        SparkSession spark = SparkUtil.sparkSession;
        JavaSparkContext context= SparkUtil.context;

        JavaRDD<Integer> lines1 = context.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9),1);
        JavaRDD<Integer> lines2 = context.parallelize(Arrays.asList(8,7,6,5,4,3,2,1),1);
//        union
        System.out.println(lines1.union(lines2).collect());
//        intersection
        System.out.println(lines1.intersection(lines2).collect());

//        substr
        System.out.println(lines1.subtract(lines2).collect());
    }
}
