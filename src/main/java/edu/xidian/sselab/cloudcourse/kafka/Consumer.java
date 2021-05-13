package edu.xidian.sselab.cloudcourse.kafka;

import edu.xidian.sselab.cloudcourse.redis.ClientRedis;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Properties;
import java.util.Set;


@Component
public class Consumer implements  Runnable{

	private  String topicName;

	public ClientRedis clientRedis;

	public Consumer(){}

	public Consumer(String topic, ClientRedis clientRedis){
		topicName = topic;
		this.clientRedis = clientRedis;
	}

	public Consumer(String topicName){
		this.topicName = topicName;
		this.clientRedis = new ClientRedis();
	}

	@SneakyThrows
	@Override
	//@Async("threadExecutor")
	public void run() {
		this.consume();
		//for (int i=0;i<1000;i++){
		//	System.out.println("number: "+i);
		//	Thread.sleep(1000);
		//}
	}


	public  void consume(){
		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.131.143:9092,192.168.131.146:9092,192.168.131.145:9092");//kafka clousterIP
		props.put("group.id", "test");
		props.put("enable.auto.commit", "true");
		props.put("auto.offset.reset", "earliest");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList(topicName));

		//consume record
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(500);
			if (records.isEmpty()){
				continue;
			}
			System.out.println("Get new amount of records :"+records.count());

			//在插入新数据之前，判断已经缓存的数据数目
			//如果足够多，优先统一发给HBase
			//发送完成后清空集合
			if (this.clientRedis.getSetSize()>=1000){
				//System.out.println(" redis get set size >1000");
				Set<String> curRecords = this.clientRedis.getRecord(this.clientRedis.getKey());
				this.clientRedis.publishRecords(curRecords);
				this.clientRedis.clearSet();
			}
			//把旧数据发送给HBase存储后，在存入从kafka新收到的数据
			this.clientRedis.dataFromKafka2RDB(records);
		}
	}



	public static void main(String[] args) {
		new Consumer("Tester0").consume();
	}
}
