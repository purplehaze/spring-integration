package net.smart4life.springintegration.amqpretry;

import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.handler.LoggingHandler.Level;

import lombok.extern.slf4j.Slf4j;

@Configuration
@Slf4j
@Profile("receiver-simple")
public class SimpleFlowConfig {
	
	@Bean
	public IntegrationFlow simpleFlow(ConnectionFactory connectionFactory, 
			@Qualifier("itemCreatedQueue") Queue itemCreatedQueue) {
		return IntegrationFlows
				.from(Amqp.inboundAdapter(connectionFactory, itemCreatedQueue))
				.log(Level.INFO)
				.handle((p, h) -> {
					String num = (String) p;
					log.info("processed msg={}", num);
					return p;
				})
				.log(Level.INFO, m -> "End of " + m + " processing")
				.get();
	}


	// @formatter:on
}
