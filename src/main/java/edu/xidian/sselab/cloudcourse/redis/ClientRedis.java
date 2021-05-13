package edu.xidian.sselab.cloudcourse.redis;

/**
 * Created by cx on 2017/9/20.
 */


import lombok.Data;
import lombok.ToString;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.stereotype.Component;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import java.util.Set;

@Data
@ToString
public class ClientRedis {
    private  Jedis jedis;
    private  Pipeline pipeline;
    private  String key = "05132021";
    private  String hostIP = "192.168.131.143";
    private  String mainChannel = "RawDataFromKafka";

    public ClientRedis(){
        this.setRelation(this.hostIP);
    }

    public  ClientRedis(String hostIP){
        this.setRelation(hostIP);

    }


    //与redis建立连接
    public  void setRelation(String hostIP){
        int port = 6379;
        Jedis aJedis = new Jedis(hostIP,port);
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
    public  void  dataFromKafka2RDB(ConsumerRecords<String, String> records){
        for(ConsumerRecord<String,String> record:records){
            this.pipeline.sadd(this.key,record.value());
        }
        this.pipeline.sync();
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
