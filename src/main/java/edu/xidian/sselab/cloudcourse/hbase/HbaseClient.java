package edu.xidian.sselab.cloudcourse.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class HbaseClient {

    private Configuration configuration;

    private Connection connection;

    @Autowired
    public HbaseClient() {
        configuration = new Configuration();
        configuration = new Configuration();
        //这里是HBase连接配置，只需要改一下主机名即可，不需要改变端口
        configuration.set("hbase.zookeeper.quorum","39.103.190.217,39.108.101.177,8.142.69.187");
        configuration.set("hbase.zookeeper.property.clientPort","2181");
    }

    public Connection getConnection() {
        if (connection == null || connection.isClosed()) {
            try {
                connection = ConnectionFactory.createConnection(configuration);
            } catch (IOException e) {
                System.out.println("HBase数据库连接关闭，尝试重新连接失败!");
                e.printStackTrace();
            }
        }
        return connection;
    }

    public Table getTableByName(String tableName) {
        try {
            return connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            System.out.println("连接数据库表失败!");
            e.printStackTrace();
        }
        return null;
    }
    
}
