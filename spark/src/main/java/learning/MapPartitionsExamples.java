package learning;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;
import utils.SparkUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * 作者    吴振涛
 * 描述
 * 创建时间 2018年01月15日
 * 任务时间
 * 邮件时间
 */
public class MapPartitionsExamples {
    public static void main(String[] args) {
        SparkConf sparkConf = SparkUtil.sparkConf;
        SparkSession spark = SparkUtil.sparkSession;
        JavaSparkContext context= SparkUtil.context;
        //mapPartitionsWithIndex与mapPartitions差不多   就是多了index
        JavaRDD<Integer> lines = context.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9),3);
        System.out.println(lines.collect());
        System.out.println("分区个数:"+lines.partitions().size());
        JavaRDD<Integer> resultLines1 = lines.mapPartitions(new FlatMapFunction<Iterator<Integer>, Integer>() {
            @Override
            public Iterator<Integer> call(Iterator<Integer> integerIterator) throws Exception {
                List<Integer> result = new ArrayList<>();
                int sum=0;
                while (integerIterator.hasNext()){
                    sum+=integerIterator.next();
                }
                result.add(sum);
                Iterator<Integer> kk = result.iterator();
                return kk;
            }
        });
        System.out.println(resultLines1.collect());
        JavaRDD<String> resultLines2 = lines.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer integer, Iterator<Integer> integerIterator) throws Exception {
                List<String> result = new ArrayList<>();
                int sum=0;
                while (integerIterator.hasNext()){
                    sum+=integerIterator.next();
                }
                result.add(integer+" "+sum);
                Iterator<String> kk = result.iterator();
                return kk;
            }
        },false);
        System.out.println(resultLines2.collect());
    }
}
