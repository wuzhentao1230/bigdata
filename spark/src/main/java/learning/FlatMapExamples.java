package learning;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
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
public class FlatMapExamples {
    public static void main(String[] args) {
        SparkConf sparkConf = SparkUtil.sparkConf;
        SparkSession spark = SparkUtil.sparkSession;
        JavaSparkContext context= SparkUtil.context;
        //一下是新建了一个rdd并进行了flatMap操作
        //map函数会对每一条输入进行指定的操作，然后为每一条输入返回一个对象；而flatMap函数则是两个操作的集合——正是“先映射后扁平化”：
        JavaRDD<Integer> lines = context.parallelize(Arrays.asList(1,2,3,4),1);
        System.out.println(lines.collect());
        JavaRDD<Integer> reslut=lines.flatMap(new FlatMapFunction<Integer, Integer>() {
            @Override
            public Iterator<Integer> call(Integer integer) throws Exception {
                List<Integer> list = new ArrayList<>();
                for (int i = 1; i <= integer ; i++) {
                    list.add(i);
                }
                Iterator<Integer> kk = list.iterator();
                return kk;

            }
        });
        System.out.println(reslut.collect());
    }
}
