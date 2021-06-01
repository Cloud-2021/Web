package edu.xidian.sselab.cloudcourse.kafka;

import org.apache.kafka.clients.producer.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.Scanner;


public class ProducerExample {

	private String topicName="cloude";

	public ProducerExample(String name){
		topicName = name;
	}
	public ProducerExample(){}

	public static void main(String[] args) throws IOException, InterruptedException {
		ProducerExample producerExample = new ProducerExample("cloude");
		producerExample.sendRecords();
	}

	public void sendRecords() throws IOException, InterruptedException {
		Properties props = new Properties();
		props.put("bootstrap.servers", "123.57.186.221:9092,123.56.224.75:9092,8.140.46.221:9092");//kafka clusterIP
		//props.put("bootstrap.servers","8.140.46.221:9092");
		props.put("acks", "1");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		String DATA_PATH = System.getProperty("user.dir")+"\\src\\main\\resources\\record.json";
		BufferedReader br =  new BufferedReader(new FileReader(DATA_PATH));//record file path
		Producer<String, String> producer = new KafkaProducer<>(props);
		if(producer!=null){
			System.out.println("Create producer success.\n");
		}

		int i = 0;//message key
		String record = null;
		//send record to kafka
		int j = 0;
		while( j<4000 &&  (record = br.readLine())!=null) {
			System.out.println("sent record: "+record);
			producer.send(new ProducerRecord<String, String>(topicName, Integer.toString(i), record), new Callback() {
				public void onCompletion(RecordMetadata metadata, Exception e) {
					if (e != null )
						e.printStackTrace();
					System.out.println("The offset of the record we just sent is: " + metadata.offset());
					System.out.println("Sending success.");
				}
			});
			i++;
			j++;
			Thread.sleep(1500);
		}
		producer.close();
	}



}
