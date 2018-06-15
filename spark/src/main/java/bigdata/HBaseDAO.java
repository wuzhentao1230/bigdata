package bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 作者    吴振涛
 * 描述
 * 创建时间 2018年02月02日
 * 任务时间
 * 邮件时间
 */
public class HBaseDAO {
    static Configuration conf= HBaseConfiguration.create();
    static{
        //zookeeper地址
        conf.set("hbase.zookeeper.quorum", "192.168.21.3");
        //zookeeper端口
        conf.set("hbase.zookeeper.property.clientPort", "2181");
    }

    public static void createTable(String tablename,String columnFamily) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
        HBaseAdmin admin=new HBaseAdmin(conf);
        if(admin.tableExists(tablename)){
            System.out.println("Table exists!");
        }else{
            HTableDescriptor tableDesc=new HTableDescriptor(TableName.valueOf(tablename));
            tableDesc.addFamily(new HColumnDescriptor(columnFamily));
            admin.createTable(tableDesc);
            System.out.println("create table success!");
        }
        admin.close();
    }

    public static boolean deleteTable(String tablename) throws MasterNotRunningException, ZooKeeperConnectionException, IOException{
        HBaseAdmin admin=new HBaseAdmin(conf);
        if(admin.tableExists(tablename)){
            try {
                admin.disableTable(tablename);
                admin.deleteTable(tablename);
            } catch (Exception e) {
                e.printStackTrace();
                admin.close();
                return false;
            }
        }
        admin.close();
        return true;
    }

    public static void putRow(HTable table, String rowKey, String columnFamily, String key, String value) throws IOException{
        Put rowPut=new Put(Bytes.toBytes(rowKey));
        rowPut.add(columnFamily.getBytes(),key.getBytes(),value.getBytes());
        table.put(rowPut);
        System.out.println("put '"+rowKey+"', '"+columnFamily+":"+key+"', '"+value+"'");
    }

    public static Result getRow(HTable table, String rowKey) throws IOException{
        Get get=new Get(Bytes.toBytes(rowKey));
        Result result=table.get(get);
        System.out.println("Get: "+result);
        return result;
    }

    public static void deleteRow(HTable table,String rowKey) throws IOException{
        Delete delete=new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);
        System.out.println("Delete row:"+rowKey);
    }

    public static ResultScanner scanAll(HTable table) throws IOException{
        Scan s=new Scan();
        ResultScanner rs=table.getScanner(s);
        return rs;
    }

    public static ResultScanner scanRange(HTable table,String startrow,String endrow) throws IOException{
        Scan s=new Scan(Bytes.toBytes(startrow),Bytes.toBytes(endrow));
        ResultScanner rs=table.getScanner(s);
        return rs;
    }

    public static ResultScanner scanFilter(HTable table,String startrow,Filter filter) throws IOException{
        Scan s=new Scan(Bytes.toBytes(startrow),filter);
        ResultScanner rs=table.getScanner(s);
        return rs;
    }

    public static void main(String[] args) throws IOException{
        HTable table = new HTable(conf, "wuzhentao");
      ResultScanner rs = HBaseDAO.scanRange(table, "2015-08-19*", "2015-08-21*");
//    	ResultScanner rs = HBaseDAO.scanRange(table, "100001", "100004");
//    	ResultScanner rs = HBaseDAO.scanAll(table);
//
//    	for(Result row:rs) {
//    		System.out.println("Row: "+new String(row.getRow()));
//    		for(Entry<byte[], byte[]> entry:row.getFamilyMap("fam1".getBytes()).entrySet()){
//    			String key=new String(entry.getKey());
//    			String value=new String(entry.getValue());
//    			System.out.format("COLUMN \t fam1:%s \t %s \n", key,value);
//    		}
//    	}
//      HBaseDAO.deleteTable("wuzhentao");
//        HBaseDAO.createTable("wuzhentao", "nannan");
//		HBaseDAO.putRow(table, "name3", "nannan", "shi", "ni hao ,what,s your name");
//    	HBaseDAO.deleteRow(table, "100001");
    	HBaseDAO.getRow(table, "name2");
//    	HBaseDAO.deleteTable("tab1");
        table.close();
    }
}