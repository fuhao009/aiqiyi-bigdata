package com.study.Utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class HBaseUtils {
    // 默认为空
    HBaseAdmin admin = null;
    Configuration configration = null;
    private HBaseUtils(){
        configration = new Configuration();
        configration.set("hbase.zookeeper.quorum","master:2181");
        configration.set("hbase.rootdir","hdfs://master/hbase");
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

    public HTable getTable(String tableName){
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
     * 根据表名输入条件获取 Hbase 的记录数
     * tableName 表名
     * condition row前缀字段：如：2017001 2017002 如果要匹配两条记录只需要填写2017即可
     */
    public Map<String, Long> query(String tableName, String condition)throws IOException {
        Map<String, Long> map = new HashMap<>();
        HTable table = getTable(tableName);
        // 列族
        String cf = "info";
        // 列名
        String qualifier = "click_count";
        // 创建扫描器
        Scan scan = new Scan();
        // 设置过滤器
        Filter filter = new PrefixFilter(Bytes.toBytes(condition));
        scan.setFilter(filter);
        // 执行扫描器
        ResultScanner rs = table.getScanner(scan);
        // 将表结果
        for (Result result : rs) { ;
            // 获取行ID
            String row = Bytes.toString(result.getRow());
            // Hbase中保存的什么格式这里必须是对应的格式,不然会识别不到
            // 获取值
            Long clickCount = Bytes.toLong(result.getValue(cf.getBytes(),qualifier.getBytes()));
            map.put(row,clickCount);
            }
        return map;
    }
    public static void main(String[] args) throws IOException {
        Map<String, Long> map = null;
            // 静态类需要这样调用
            map = HBaseUtils.getInstance().query("category_search_clickcount","20180529");
            for (Map.Entry<String, Long> entry : map.entrySet()) {
                System.out.println(entry.getKey() + " : " + entry.getValue());
            }
    }
}
