package net.smart4life.springintegration.amqpretry;

import java.util.HashMap;
import java.util.Map;

import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.retry.RepublishMessageRecoverer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.messaging.MessageHeaders;

@Configuration
@Profile("receiver")
public class RetryAndErrorConfig {
	// @formatter:off
	public static final String RABBIT_PREFIX = "romans.play.";
	
	public static final String EXCHANGE_NOT_PROCESSED = RABBIT_PREFIX + "not.processed";
	public static final String QUEUE_NOT_PROCESSED = RABBIT_PREFIX + "not.processed";
	
	public static final String EXCHANGE_FIRST_STAGE_RETRY = RABBIT_PREFIX+"mlmtech.firststage.retry";
	private static final String QUEUE_FIRST_STAGE_RETRY = RABBIT_PREFIX+"mlmtech.firststage.retry";
	private static final Integer FIRST_STAGE_RETRY_TTL = 5000;
	
	public static final String EXCHANGE_SECOND_STAGE_RETRY = RABBIT_PREFIX+"mlmtech.secondstage.retry";
	private static final String QUEUE_SECOND_STAGE_RETRY = RABBIT_PREFIX+"mlmtech.secondstage.retry";
	private static final Integer SECOND_STAGE_RETRY_TTL = 10000;
	
	public static final String EXCHANGE_FROM_M3 = RABBIT_PREFIX+"from.m3";
	
	
	//////////////////////////////////////////////////// first stage retry config
	@Bean
	public static TopicExchange firstStageRetryExchange() {
		return new TopicExchange(EXCHANGE_FIRST_STAGE_RETRY, true, false);
	}
	
	@Bean
	public static Queue firstStageRetryQueue() {
		Map<String, Object> args = new HashMap<>();
		args.put(AmqpConstants.DLX_HEAD, EXCHANGE_FROM_M3);
		args.put(AmqpConstants.DLX_TTL_HEAD, FIRST_STAGE_RETRY_TTL);
		return new Queue(QUEUE_FIRST_STAGE_RETRY, true, false, false, args);
	}
	
	@Bean
	public static Binding firstStageRetryBinding() {
		return BindingBuilder
				.bind(firstStageRetryQueue())
				.to(firstStageRetryExchange())
				.with("#");
	}
	
	@Bean
	RepublishMessageRecoverer firstStageRetryMessagePublisher(AmqpTemplate amqpTemplate) {
		RepublishMessageRecoverer publisher = new RepublishMessageRecoverer(amqpTemplate, EXCHANGE_FIRST_STAGE_RETRY);
		publisher.setErrorRoutingKeyPrefix("");
		return publisher;
	}
	
	//////////////////////////////////////////////////// second stage retry config
	@Bean
	public static TopicExchange secondStageRetryExchange() {
		return new TopicExchange(EXCHANGE_SECOND_STAGE_RETRY, true, false);
	}
	
	@Bean
	public static Queue secondStageRetryQueue() {
		Map<String, Object> args = new HashMap<>();
		args.put(AmqpConstants.DLX_HEAD, EXCHANGE_FROM_M3);
		args.put(AmqpConstants.DLX_TTL_HEAD, SECOND_STAGE_RETRY_TTL);
		return new Queue(QUEUE_SECOND_STAGE_RETRY, true, false, false, args);
	}
	
	@Bean
	public static Binding secondStageRetryBinding() {
		return BindingBuilder
				.bind(secondStageRetryQueue())
				.to(secondStageRetryExchange())
				.with("#");
	}
	
	@Bean
	RepublishMessageRecoverer secondStageRetryMessagePublisher(AmqpTemplate amqpTemplate) {
		RepublishMessageRecoverer publisher = new RepublishMessageRecoverer(amqpTemplate, EXCHANGE_SECOND_STAGE_RETRY);
		publisher.setErrorRoutingKeyPrefix("");
		return publisher;
	}
	
	//////////////////////////////////////////////////////////// NOT PROCESSED ////////////////////////////////////////////
	
	@Bean
	public static TopicExchange notProcessedExchange() {
		return new TopicExchange(EXCHANGE_NOT_PROCESSED, true, false);
	}
	
	@Bean
	public static Queue notProcessedQueue() {
		return new Queue(QUEUE_NOT_PROCESSED, true, false, false);
	}
	
	@Bean
	public static Binding notProcessedBinding(TopicExchange notProcessedExchange, Queue notProcessedQueue) {
		return BindingBuilder
				.bind(notProcessedQueue)
				.to(notProcessedExchange)
				.with("#");
	}
	
	@Bean
	RepublishMessageRecoverer notProcessedMessagePublisher(AmqpTemplate amqpTemplate) {
		RepublishMessageRecoverer publisher = new RepublishMessageRecoverer(amqpTemplate, EXCHANGE_NOT_PROCESSED);
		publisher.setErrorRoutingKeyPrefix("");
		return publisher;
	}
	
	
	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	
	
	@Bean
	public ErrorChannelHandlingServiceActivator errorHandlingServiceActivator() {
		return new ErrorChannelHandlingServiceActivator();
	}
	
	@Bean
	public IntegrationFlow errorFlow(AmqpTemplate amqpTemplate) {
		return IntegrationFlows
				.from(MessageHeaders.ERROR_CHANNEL)
				.handle(errorHandlingServiceActivator())
				.get();
	}
	
	// @formatter:on
}
