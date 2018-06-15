package bigdata;

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
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Serializable;
import scala.Tuple2;

import java.io.IOException;
import java.util.List;

public class sparkusehbase implements Serializable {
    String convertScanToString(Scan scan) throws IOException {
        ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
        return Base64.encodeBytes(proto.toByteArray());
    }

    public void start() {
        SparkConf sparkConf =  new SparkConf().setMaster("local").setAppName("taotao");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);


        Configuration conf = HBaseConfiguration.create();

        Scan scan = new Scan();
        //scan.setStartRow(Bytes.toBytes("195861-1035177490"));
        //scan.setStopRow(Bytes.toBytes("195861-1072173147"));
        scan.addFamily(Bytes.toBytes("pic_col"));
        scan.addColumn(Bytes.toBytes("pic_col"), null);

        try {

            String tableName = "pic_test2";
            conf.set(TableInputFormat.INPUT_TABLE, tableName);
//            conf.set(TableInputFormat.SCAN, convertScanToString(scan));


            JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = sc.newAPIHadoopRDD(conf,
                    TableInputFormat.class, ImmutableBytesWritable.class,
                    Result.class);

            JavaPairRDD<String, Integer> levels = hBaseRDD.mapToPair(
                    new PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, Integer>() {
                        @Override
                        public Tuple2<String, Integer> call(Tuple2<ImmutableBytesWritable, Result> immutableBytesWritableResultTuple2) throws Exception {
                            byte[] o = immutableBytesWritableResultTuple2._2().getValue(Bytes.toBytes("pic_col"), null);
                            if (o != null) {
                                return new Tuple2<String, Integer>(new String(o), 1);
                            }
                            return null;
                        }
                    });

            JavaPairRDD<String, Integer> counts = levels.reduceByKey(
                    new Function2<Integer, Integer, Integer>() {
                        @Override
                        public Integer call(Integer i1, Integer i2) {
                            return i1 + i2;
                        }
                    });

            List<Tuple2<String, Integer>> output = counts.collect();
            for (Tuple2 tuple : output) {
                System.out.println(tuple._1() + ": " + tuple._2());
            }

            sc.stop();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        new sparkusehbase().start();
        System.exit(0);
    }
}
