package bigdata;

/**
 * 作者    吴振涛
 * 描述
 * 创建时间 2018年02月02日
 * 任务时间
 * 邮件时间
 */

import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.SparkSession;
import scala.Serializable;
import scala.Tuple2;

import java.io.IOException;

/**
 * User: iteblog
 * Date: 14-6-27
 * Time: 下午5:18
 *blog: http://www.iteblog.com
 *
 * Usage: bin/spark-submit --master yarn-cluster --class com.iteblog.spark.SparkFromHbase
 * --jars /home/q/hbase/hbase-0.96.0-hadoop2/lib/htrace-core-2.01.jar,
 * /home/q/hbase/hbase-0.96.0-hadoop2/lib/hbase-common-0.96.0-hadoop2.jar,
 * /home/q/hbase/hbase-0.96.0-hadoop2/lib/hbase-client-0.96.0-hadoop2.jar,
 * /home/q/hbase/hbase-0.96.0-hadoop2/lib/hbase-protocol-0.96.0-hadoop2.jar,
 * /home/q/hbase/hbase-0.96.0-hadoop2/lib/hbase-server-0.96.0-hadoop2.jar
 * ./spark_2.10-1.0.jar
 */
public class SparkFromHbase implements Serializable {

    /**
     * copy from org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil
     *
     * @param scan
     * @return
     * @throws IOException
     */
    String convertScanToString(Scan scan) throws IOException {
        ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
        return Base64.encodeBytes(proto.toByteArray());
    }

    public void start() {
        SparkConf sparkConf =  new SparkConf().setMaster("local").setAppName("taotao");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        SparkSession sparkSession = SparkSession
                .builder()
                .config(sparkConf)
                .getOrCreate();

        Configuration conf = HBaseConfiguration.create();
        //zookeeper地址
        conf.set("hbase.zookeeper.quorum", "192.168.21.3");
        //zookeeper端口
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        Scan scan = new Scan();
        //scan.setStartRow(Bytes.toBytes("195861-1035177490"));
        //scan.setStopRow(Bytes.toBytes("195861-1072173147"));
//        scan.addFamily(Bytes.toBytes("nannan"));
        scan.addColumn(Bytes.toBytes("json"), Bytes.toBytes("data"));

        try {

            String tableName = "hbasetest";
            conf.set(TableInputFormat.INPUT_TABLE, tableName);
            conf.set(TableInputFormat.SCAN, convertScanToString(scan));


            JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = sc.newAPIHadoopRDD(conf,
                    TableInputFormat.class, ImmutableBytesWritable.class,
                    Result.class);


//            JavaPairRDD<String, Integer> levels = hBaseRDD.mapToPair(
//                    new PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, Integer>() {
//                        @Override
//                        public Tuple2<String, Integer> call(Tuple2<ImmutableBytesWritable, Result> immutableBytesWritableResultTuple2) throws Exception {
//                            byte[] o = immutableBytesWritableResultTuple2._2().getValue(Bytes.toBytes("nannan"), Bytes.toBytes("shi"));
//                            if (o != null) {
//                                return new Tuple2<String, Integer>(new String(o), 1);
//                            }
//                            return null;
//                        }
//                    });
//
//            JavaPairRDD<String, Integer> counts = levels.reduceByKey(
//                    new Function2<Integer, Integer, Integer>() {
//                        @Override
//                        public Integer call(Integer i1, Integer i2) {
//                            return i1 + i2;
//                        }
//                    });
//
//            List<Tuple2<String, Integer>> output = counts.collect();
//            for (Tuple2 tuple : output) {
//                System.out.println(tuple._1() + ": " + tuple._2());
//            }




                    JavaRDD<String> result = hBaseRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, String>() {
                        @Override
                        public String call(Tuple2<ImmutableBytesWritable, Result> immutableBytesWritableResultTuple2) throws Exception {
                            byte[] o = immutableBytesWritableResultTuple2._2().getValue(Bytes.toBytes("json"), Bytes.toBytes("data"));
                            if (o != null) {
//                                JsonObject aNew = gson.fromJson(, JsonObject.class);
//                                New new1=new New(aNew.get("date").getAsString(),aNew.get("name").getAsString(),aNew.get("judao").getAsString())
                                return new String(o);
                            }
                            return null;
                        }
                    });
//            JavaRDD<New> nrdd=result.map(new Function<String, New>() {
//                @Override
//                public New call(String s) throws Exception {
//                    Gson gson=new Gson();
//                    New aNew = gson.fromJson(s, New.class);
////                    New new1=new New(aNew.get("date").getAsString(),aNew.get("name").getAsString(),aNew.get("judao").getAsString());
//                    return aNew;
//                }
//            });
//
//            Dataset<Row> wDF = sparkSession.createDataFrame(nrdd, New.class);
//            wDF.createOrReplaceTempView("News");
//            Dataset<Row> nDF = sparkSession.sql("SELECT name,date,judao FROM News Where name like \"%百度%\"");
//            Encoder<String> stringEncoder = Encoders.STRING();
//            Dataset<String> newByIndexDF = nDF.map(new MapFunction<Row, String>() {
//                public String call(Row row) throws Exception {
//                    return "Name: " + row.getString(0);
//                }
//            }, stringEncoder);
//            nDF.show();
//            System.out.println("-----------------------------------");
//            newByIndexDF.show();



            sc.stop();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        new SparkFromHbase().start();
        System.exit(0);
    }
}