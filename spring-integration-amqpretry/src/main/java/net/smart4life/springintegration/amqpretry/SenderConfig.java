package net.smart4life.springintegration.amqpretry;

import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.amqp.outbound.AmqpOutboundEndpoint;
import org.springframework.integration.annotation.MessagingGateway;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.MessageChannel;

import lombok.extern.slf4j.Slf4j;

@Configuration
@Slf4j
@Profile("sender-aaaaa")
public class SenderConfig {
	@Bean
	@ServiceActivator(inputChannel = "amqpOutboundChannel")
	public AmqpOutboundEndpoint amqpOutbound(AmqpTemplate amqpTemplate) {
		AmqpOutboundEndpoint outbound = Amqp.outboundAdapter(amqpTemplate)
				.exchangeName(RetryAndErrorConfig.EXCHANGE_FROM_M3)
				.routingKey(AmqpConfig.QUEUE_ITEM_CREATED)
				.get();
		return outbound;
	}

	@Bean
	public MessageChannel amqpOutboundChannel() {
		return new DirectChannel();
	}

	@MessagingGateway(defaultRequestChannel = "amqpOutboundChannel")
	public interface RabbitItemGateway {
		void sendToRabbit(String text);
	}

}
