package edu.xidian.sselab.cloudcourse.redis;

/**
 * Created by cx on 2017/9/20.
 */


import edu.xidian.sselab.cloudcourse.domain.Record;
import edu.xidian.sselab.cloudcourse.hbase.HBaseConf;
import edu.xidian.sselab.cloudcourse.hbase.HBaseCreateOP;
import edu.xidian.sselab.cloudcourse.hbase.HBaseInsert;
import lombok.Data;
import lombok.ToString;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.stereotype.Component;
import redis.clients.jedis.Client;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Data
@ToString
public class ClientRedis {
    private  Jedis jedis;
    private  Pipeline pipeline;
    private  String key = "cloud0";
    private  String hostIP = "1.116.69.183";
    private  int port = 6333;
    private  String mainChannel = "RawDataFromKafka";
    //private  Jedp

    public ClientRedis(){
        this.setRelation(this.hostIP);
    }

    public  ClientRedis(String hostIP){
        this.setRelation(hostIP);
    }



    //与redis建立连接
    public  void setRelation(String hostIP){
        Jedis aJedis = new Jedis(hostIP,this.port);
        if(aJedis!=null){
            this.jedis = aJedis;
            this.pipeline = jedis.pipelined();
            System.out.println("Connection to host: "+hostIP+"  redis established.");
        }
        else {
            System.out.println("Cannot connect to redis server.");
        }
    }

    public void  publishRecords(Set<String> records){
        for(String string:records){
            this.pipeline.publish(this.mainChannel,string);
        }
        this.pipeline.sync();
    }

    public void clearSet(){
        this.jedis.del(this.key);
    }

    public long getSetSize(){
        return  this.jedis.scard(this.key);
    }

    //插入单条kafka数据
    public void dataFromKafka2RDB(ConsumerRecord<String,String> singleRecord){
        this.jedis.sadd(this.key,singleRecord.value());
    }

    //使用pipeline批量插入大量kafka数据集合
    public void dataFromKafka2RDB(ConsumerRecords<String, String> records){
        for(ConsumerRecord<String,String> record:records){
            this.pipeline.sadd(this.key,record.value());
            System.out.println("Redis insertion: "+record.value()+" completed.");
        }
        this.pipeline.sync();
    }

    public static void main(String[] args) throws IOException {
        ClientRedis clientRedis = new ClientRedis();
        clientRedis.insertIntoHBase();
        System.out.println(clientRedis.getSetSize());
    }

    public void insertIntoHBase() throws IOException {

        Connection connection = HBaseConf.getConnection();
        if (connection!=null){
            System.out.println("connection established.");

        Set<String> curRecords = this.getRecord(this.getKey());
        List<Record> curRecordsList = new ArrayList<>();
        for (String record:curRecords){
            curRecordsList.add(Record.json2Record(record));
        }
        List<String> familys = new ArrayList<>();
        familys.add("info");
        HBaseCreateOP.CreateTable("Record" , familys);
        HBaseInsert.insertRecordsToHBase(curRecordsList);
        //this.clearSet(); //清空已经插入HBase的数据
        }else{
            System.out.println("HBase connection failed.");
        }
    }

    public Set<String> getRecord(String skey){
        Set<String> record = jedis.smembers(skey);
        return  record;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }
}
