package net.smart4life.springintegration.amqpretry;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.integration.annotation.IntegrationComponentScan;

@IntegrationComponentScan
@SpringBootApplication
public class SpringIntegrationPlayApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringIntegrationPlayApplication.class, args);
	}
}
