package com.study.spark.project.Utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;


// 配置类
public class HBaseUtils {
    // 默认为空
    HBaseAdmin admin = null;
    Configuration configration = null;
    private HBaseUtils(){
        configration = new Configuration();
        configration.set("hbase.zookeeper.quorum", "master1:2181");
        configration.set("hbase.rootdir", "hdfs://master1/hbase");
        try {
            Connection connection = ConnectionFactory.createConnection(configration);
            Admin admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    // 默认为空
    private static HBaseUtils instance = null;
    // 判断表是否存在
    public static synchronized HBaseUtils getInstance() {
        if (null == instance) {
            instance = new HBaseUtils();
        }
        // 如果存在返回表
        return instance;
    }

    /**
     * 根据表名获取htable实例
     * @param tableName
     * @return
     */
    public HTable getHtable(String tableName){
        HTable table = null;
        try {
            Connection connection = ConnectionFactory.createConnection(configration);
            TableName htd = TableName.valueOf(tableName);
            table = (HTable) connection.getTable(htd);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return table;

    }

    /**
     * 添加数据到hbase里面
     * @param tableName 表名
     * @param rowKey 对应key的值
     * @param cf    hbase列簇
     * @param colum hbase对应的列
     * @param value hbase对应的值
     */
    public  void put(String tableName,String rowKey,String cf,String colum,String value){
        HTable table = getHtable(tableName);
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(colum),Bytes.toBytes(value));
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        String tableName = "category_clickcount1";
        String rowkey="20171122_3";
        String cf = "info";
        String colum = "cagegory_click_count";
        String value = "300";
        HBaseUtils.getInstance().put(tableName,rowkey,cf,colum,value);
    }
}
