package edu.xidian.sselab.cloudcourse.kafka;

import edu.xidian.sselab.cloudcourse.redis.ClientRedis;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.Set;


@Component
public class Consumer implements  Runnable{

	private  String topicName="cloude";

	public ClientRedis clientRedis = new ClientRedis();

	public Consumer(){
	}

	public Consumer(String topic, ClientRedis clientRedis){
		topicName = topic;
		this.clientRedis = clientRedis;
	}

	public Consumer(String topicName){
		this.topicName = topicName;
	}

	@SneakyThrows
	@Override
	//@Async("threadExecutor")
	public void run() {
		this.consume();
	}

	public static void main(String[] args) throws IOException {
		new Consumer("cloude").consume();
	}


	public  void consume() throws IOException {
		Properties props = new Properties();
		props.put("bootstrap.servers", "123.57.186.221:9092,123.56.224.75:9092,8.140.46.221:9092");//kafka clousterIP
		//props.put("bootstrap.servers","8.140.46.221:9092");
		props.put("group.id", "test");
		props.put("enable.auto.commit", "true");
		props.put("auto.offset.reset", "earliest");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList(topicName));

		//consume record
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			if (records.isEmpty() || records==null){
				continue;
			}
			System.out.println("Get new amount of records :"+records.count());
			this.clientRedis.dataFromKafka2RDB(records);
			//if (this.clientRedis.getSetSize()>=100){
			//	System.out.println(" redis get set size >1000");
				//Set<String> curRecords = this.clientRedis.getRecord(this.clientRedis.getKey());
				//this.clientRedis.publishRecords(curRecords);
				//this.clientRedis.clearSet();
			this.clientRedis.insertIntoHBase();
			//}
		}
	}

}
