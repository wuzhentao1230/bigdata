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
public class JoinAndCogroupExamples {
    public static void main(String[] args) {
        SparkConf sparkConf = SparkUtil.sparkConf;
        SparkSession spark = SparkUtil.sparkSession;
        JavaSparkContext context= SparkUtil.context;
        List<Integer> data1 = Arrays.asList(1,2,4);
        List<Integer> data2 = Arrays.asList(1,5,6);
        JavaRDD<Integer> distData1= context.parallelize(data1);
        JavaRDD<Integer> distData2= context.parallelize(data2);

        JavaPairRDD<Integer, Integer> firstRDD1 = distData1.mapToPair(new PairFunction<Integer, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Integer integer) throws Exception {
                if (integer%2==0) {
                    return new Tuple2(1, integer * integer);
                }else {
                    return new Tuple2(2, integer * integer);
                }
            }
        });
        JavaPairRDD<Integer, Integer> firstRDD2 = distData2.mapToPair(new PairFunction<Integer, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Integer integer) throws Exception {
                if (integer%2==0) {
                    return new Tuple2(1, integer * integer);
                }else {
                    return new Tuple2(2, integer * integer);
                }
            }
        });
        System.out.println("生成的原始RDD1"+firstRDD1.collect());
        System.out.println("生成的原始RDD2"+firstRDD2.collect());
//        把相同的key做遍历
        System.out.println("Join->"+firstRDD1.join(firstRDD2).collect());

//        把相同key的元组合并
        System.out.println("cogroup->"+firstRDD1.cogroup(firstRDD2).collect());

//        cartesian
        System.out.println("cartesian->"+firstRDD1.cartesian(firstRDD2).collect());
    }
}
