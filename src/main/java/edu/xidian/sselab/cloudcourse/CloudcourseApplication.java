package edu.xidian.sselab.cloudcourse;

import edu.xidian.sselab.cloudcourse.kafka.Consumer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@EnableAutoConfiguration
//@ComponentScans
public class CloudcourseApplication {

	public static void main(String[] args) {
		SpringApplication.run(CloudcourseApplication.class, args);
		Thread thread = new Thread(new Consumer());
		thread.start();
	}
    
}
