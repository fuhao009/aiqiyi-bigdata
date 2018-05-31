package com.study.spark.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseUtile {
    HBaseAdmin admin = null;
    Configuration configuration = null;

    private HbaseUtile() {
        // 创建配置文件
        Configuration conf = new Configuration();
        // zookeeper地址
        configuration.set("hbase.zookerper.quorum", "master1:2181");
        // hdfs地址,必须是avtime状态
        configuration.set("hbase", "hdfs://master1:9000/hbase");
        try {
            Connection connection = ConnectionFactory.createConnection(configuration);
            Admin admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 创建空实例
    private static HbaseUtile instance = null;
    // 判断如果表(实例)存在,则返回表(实例)
    public static synchronized HbaseUtile getInstance() {
        if (null == instance) {
            // 实例化
            System.out.println("null instance");
            instance = new HbaseUtile();
        }
        // 返回实例
        return instance;
    }
    /**
     * 根据表名获取htable实例
     * @param tableName
     * @return
     */
    public HTable getHtable(String tableName) {
        HTable table = null;
        try {
            Connection connection = ConnectionFactory.createConnection(configuration);
            TableName htd = TableName.valueOf(tableName);
            table = (HTable) connection.getTable(htd);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return table;
    }

    /**
     * 上传表数据
     * @param tablenName    表名
     * @param rowKey    对应的key的值
     * @param cf    列族
     * @param colum 列名
     * @param value 值名
     */
    public void put(String tablenName,String rowKey,String cf,String colum,String value){
        Connection connection = null;
        try {
            // 创建链接
            connection = ConnectionFactory.createConnection(configuration);
            // 表名
            TableName htd = TableName.valueOf(tablenName);
            // 表描述
            Table table = connection.getTable(htd);
            // 插入值
            Put put = new Put(Bytes.toBytes(value));
            // 设置插入的列
            put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(colum), Bytes.toBytes(value));
            // 开始上传
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    public static void main(String[] args) {
        String tableName = "category_clickcount";
        String rowkey="20171122_5";
        String cf = "info";
        String colum = "cagegory_click_count";
        String value = "400";
        HbaseUtile.getInstance().put(tableName,rowkey,cf,colum,value);
    }
}
