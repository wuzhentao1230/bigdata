package util;


import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;

import java.sql.*;

/**
 * 作者    吴振涛
 * 描述
 * 创建时间 2018年01月04日
 * 任务时间
 * 邮件时间
 */
public class HiveUtil {
    private static  Log logger = LogFactory.getLog(HiveUtil.class);
    private static String driver = "org.apache.hive.jdbc.HiveDriver";
//    private static String url = "jdbc:hive2://192.168.21.3:10000/hbasedata";
//    private static String user = "hadoop2";
//    private static String password = "123456789";
    //注册驱动
    static{
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
    //获取连接
    public static Connection getConnection(String url,String user,String password){
        try {
            return DriverManager.getConnection(url,user,password);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }
    //释放资源
    public static void release(Connection conn, Statement st, ResultSet rs){
        if(rs != null){
            try {
                rs.close();
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }finally{
                rs = null;
            }
        }
        if(st != null){
            try {
                st.close();
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }finally{
                st = null;
            }
        }
        if(conn != null){
            try {
                conn.close();
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }finally{
                conn = null;
            }
        }
    }
    // 创建数据库
    public static void createDatabase(Statement stmt) throws Exception {
        String sql = "create database hbasedata";
        System.out.println("Running: " + sql);
        stmt.execute(sql);
    }
    // 查询所有数据库
    public static ResultSet  showDatabases(Statement stmt) throws Exception {
        String sql = "show databases";
        return stmt.executeQuery(sql);
    }
    // 查询所有表
    public  static ResultSet showTables(Statement stmt) throws Exception {
        String sql = "show tables";
        System.out.println("Running: " + sql);
        return stmt.executeQuery(sql);
    }
    //创建表
    public static void create(Statement stmt,String createSQL) throws Exception {
        boolean bool = stmt.execute(createSQL);
        logger.info("创建表成功");
    }
    //删除表
    public static void drop(Statement stmt,String tablename) throws Exception {
        String dropSQL = "drop table if exists "+tablename;
        stmt.execute(dropSQL);
    }
    //复制表
    public static void copy(Statement stmt,String tablename,String  prefix,String order) throws Exception {
        long startTime = System.currentTimeMillis();//获取当前时间
        String sql2 = "create table "+prefix+tablename+" as select Row_Number() over (order by "+order+") as GxhsCopyId ,* from "+tablename;
        stmt.execute(sql2);
        long endTime = System.currentTimeMillis();
        logger.info("程序复制运行时间："+(endTime-startTime)+"ms copy表成功");
    }
    //统计总量
    public static int count(Statement stmt,String tablename,String  prefix) throws Exception {
        String sql1 = "select count(*) from "+prefix+tablename;
        ResultSet rs = stmt.executeQuery(sql1);
        int num=0;
        while(rs.next()){
           num = rs.getInt(1);
        }
        return num;
    }
    //表查询
    public static ResultSet exec(Statement st, String tableName) throws SQLException {
        return  st.executeQuery("desc " + tableName);
    }
    //插入数据
    public static Boolean insert(Statement st, String insertStr) throws SQLException {
        return  st.execute(insertStr);
    }
    //导入数据
    public static void insertdata(Statement st, String tableName,String filePath) throws SQLException {
        String sql = "load data local inpath '" + filePath + "' into table " + tableName;
        st.execute(sql);
        System.out.println("load data into table success!");
    }

    public static void main(String[] args) throws Exception {
        showDatabases(getConnection("jdbc:hive2://192.168.21.3:10000/hbasedata","hadoop2","123456789").createStatement());
    }
}
