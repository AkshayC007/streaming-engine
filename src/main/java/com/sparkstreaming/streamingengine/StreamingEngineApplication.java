package com.sparkstreaming.streamingengine;

import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;

@SpringBootApplication
public class StreamingEngineApplication extends SpringBootServletInitializer implements CommandLineRunner {

	@Autowired
	StreamingService streamingService;

	public static void main(String[] args) {
		SpringApplication.run(StreamingEngineApplication.class, args);
	}

	@Override
	protected SpringApplicationBuilder configure(SpringApplicationBuilder builder) {
		return super.configure(builder);
	}

	@Override
	public void run(String... args) throws Exception {
		SparkSession sparkSession = SparkSession
				.builder()
				.appName("SparkWithSpring")
				.master("local")
				.getOrCreate();
		System.out.println("Spark Version: " + sparkSession.version());

		streamingService.wordCount();


	}

}
