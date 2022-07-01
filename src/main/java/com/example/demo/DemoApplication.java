package com.example.demo;

import com.example.demo.model.PubSubMessage;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import reactor.core.publisher.Sinks;

@SpringBootApplication
public class DemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(DemoApplication.class, args);
	}

	@Bean
	public Sinks.Many<PubSubMessage> sink() {
		return Sinks.many().multicast().onBackpressureBuffer(1000);
	}
}
