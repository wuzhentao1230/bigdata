package learning;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
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
public class FilterExamples {
    public static void main(String[] args) {
        SparkConf sparkConf = SparkUtil.sparkConf;
        SparkSession spark = SparkUtil.sparkSession;
        JavaSparkContext context= SparkUtil.context;

        //一下是新建了一个rdd并进行了Filter操作
        JavaRDD<Integer> lines = context.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9),3);
        System.out.println(lines.collect());
        JavaRDD<Integer> reslut=lines.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer integer) throws Exception {
                if (integer%2==0) {
                    return true;
                }else {
                    return false;
                }
            }
        });
        System.out.println(reslut.collect());
    }
}
