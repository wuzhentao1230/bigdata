package learning;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import utils.SparkUtil;

import java.util.Arrays;
import java.util.List;

/**
 * 作者    吴振涛
 * 描述
 * 创建时间 2018年01月15日
 * 任务时间
 * 邮件时间
 */
public class SortByKeyExamples {
    public static void main(String[] args) {
        SparkConf sparkConf = SparkUtil.sparkConf;
        SparkSession spark = SparkUtil.sparkSession;
        JavaSparkContext context= SparkUtil.context;
        List<Integer> data = Arrays.asList(1,1,2,2,1,3,4);
        JavaRDD<Integer> distData= context.parallelize(data);

        JavaPairRDD<Integer, Integer> firstRDD = distData.mapToPair(new PairFunction<Integer, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Integer integer) throws Exception {
                if (integer%2==0) {
                    return new Tuple2(1, integer * integer);
                }else {
                    return new Tuple2(2, integer * integer);
                }
            }
        });
        System.out.println("生成的原始RDD"+firstRDD.collect());
//        三个任务数来跑对key升序排序
        System.out.println("升序sortByKey后->"+firstRDD.sortByKey(true,3).collect());
//        两个任务数来跑对key降序排序
        System.out.println("降序sortByKey后->"+firstRDD.sortByKey(false,1).collect());
    }
}
