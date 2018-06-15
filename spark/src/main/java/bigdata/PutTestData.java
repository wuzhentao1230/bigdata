package bigdata;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.hadoop.hbase.client.HTable;

import java.io.IOException;
import java.util.Random;

/**
 * 作者    吴振涛
 * 描述
 * 创建时间 2018年02月06日
 * 任务时间
 * 邮件时间
 */
public class PutTestData {
    public static void main(String[] args) throws IOException {
        Random rand = new Random();
        Gson gson=new Gson();
        HBaseDAO.deleteTable("hbasetest");
        HBaseDAO.createTable("hbasetest", "json");
        HTable table = new HTable(HBaseDAO.conf, "hbasetest");
        String[] dates={"2016-01-01 10:11:31","2015-01-01 10:11:31","2017-01-01 10:11:31","2018-01-01 00:00:00","2018-01-01 10:11:31"};
        String[] names={"新浪","搜狐","腾讯","百度","百度一下你就知道"};
        String[] judaos={null,"","1","2","3"};
        for (int i = 0; i < 1000; i++) {
            JsonObject jsonObject=new JsonObject();
            jsonObject.addProperty("date",dates[rand.nextInt(5)]);
            jsonObject.addProperty("name",names[rand.nextInt(5)]);
            jsonObject.addProperty("judao",judaos[rand.nextInt(5)]);
            HBaseDAO.putRow(table, i+"", "json", "data", gson.toJson(jsonObject));
        }
        table.close();
    }

}
