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
public class GroupByKeyExamples {
    public static void main(String[] args) {
        SparkConf sparkConf = SparkUtil.sparkConf;
        SparkSession spark = SparkUtil.sparkSession;
        JavaSparkContext context = SparkUtil.context;
        List<Integer> data = Arrays.asList(1,1,2,2,1);
        JavaRDD<Integer> distData= context.parallelize(data);

        JavaPairRDD<Integer, Integer> firstRDD = distData.mapToPair(new PairFunction<Integer, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Integer integer) throws Exception {
                return new Tuple2(integer, integer*integer);
            }
        });
        System.out.println("生成的原始RDD"+firstRDD.collect());
//        groupByKey([numTasks])
        JavaPairRDD<Integer, Iterable<Integer>> secondRDD = firstRDD.groupByKey();
        System.out.println("groupby过后的RDD"+secondRDD.collect());
        context.stop();
    }
}

