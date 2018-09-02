package net.smart4life.springintegration.amqpretry;

import java.util.HashMap;
import java.util.Map;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
@Profile("receiver")
public class AmqpConfig {//NOSONAR
	public static final String QUEUE_ITEM_CREATED = RetryAndErrorConfig.RABBIT_PREFIX+"m3.item.created";
	private static final String ROUTING_KEY_SUFFIX = ".#";
	
	@Bean
	public static Map<String, Object> notProcessedDlxArguments(){
		Map<String, Object> args = new HashMap<>();
		args.put(AmqpConstants.DLX_HEAD, RetryAndErrorConfig.EXCHANGE_NOT_PROCESSED);
		return args;
	}
	
	@Bean
	public static TopicExchange fromM3Exchange() {
		return new TopicExchange(RetryAndErrorConfig.EXCHANGE_FROM_M3, true, false);
	}
	
	///////////////////////////////////////////////////////////// ITEM /////////////////////////////////////////////////////
	// Item created config
	@Bean
	@Qualifier("itemCreatedQueue")
	public static Queue itemCreatedQueue() {
		return new Queue(QUEUE_ITEM_CREATED, true, false, false, notProcessedDlxArguments());
	}
	
	@Bean
	public static Binding itemCreatedBinding(TopicExchange fromM3Exchange, Queue itemCreatedQueue) {
		return BindingBuilder
				.bind(itemCreatedQueue)
				.to(fromM3Exchange)
				.with(QUEUE_ITEM_CREATED+ROUTING_KEY_SUFFIX);
	}
}
