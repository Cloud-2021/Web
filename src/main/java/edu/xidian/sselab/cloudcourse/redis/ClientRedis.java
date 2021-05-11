package edu.xidian.sselab.cloudcourse.redis;

/**
 * Created by cx on 2017/9/20.
 */


import lombok.Data;
import lombok.ToString;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import java.util.Set;

@Data
@ToString
public class ClientRedis {
    private  Jedis jedis;
    private  Pipeline pipeline;
    private  String key = "rdb-6";
    private  String mainChannel = "RawDataFromKafka";

    public ClientRedis(){}
    public ClientRedis(String setKey){
        this.key = setKey;
    }
   
    public  ClientRedis(boolean autoSetupConnect,String hostIP){
        if(autoSetupConnect){
            this.setRelation(hostIP);
        }
    }

    public static  void  main(String[] args){
        ClientRedis clientRedis = new ClientRedis(true,"1.116.69.183");
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

    //插入单条kafka数据
    public void dataFromKafka2RDB(ConsumerRecord<String,String> singleRecord){
        this.jedis.sadd(this.key,singleRecord.value());
        this.jedis.publish(this.mainChannel,singleRecord.value());
    }

    //使用pipeline批量插入大量kafka数据集合
    //注意：数据量必须足够大，否则会暂存在Pipeline里无法及时插入redis中
    public  void  dataFromKafka2RDB(ConsumerRecords<String, String> records){
        for(ConsumerRecord<String,String> record:records){
            this.pipeline.sadd(this.key,record.value());
        }
        for(ConsumerRecord<String,String> record:records){
            this.jedis.publish(this.mainChannel,record.value());
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
