package net.smart4life.springintegration.amqpretry;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.HeadersExchange;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.config.RetryInterceptorBuilder;
import org.springframework.amqp.rabbit.retry.RepublishMessageRecoverer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.retry.interceptor.RetryOperationsInterceptor;

@Configuration
@Profile("receiver")
public class RetryAndErrorConfig {
	// @formatter:off
	public static final String RABBIT_PREFIX = "romans.play.";
	
	public static final String EXCHANGE_RETRY = RABBIT_PREFIX + "retry";
	public static final String RETRY_ARGUMENT = "retry.argument";
	
	public static final String EXCHANGE_NOT_PROCESSED = RABBIT_PREFIX + "not.processed";
	public static final String QUEUE_NOT_PROCESSED = RABBIT_PREFIX + "not.processed";
	
	private static final String QUEUE_FIRST_STAGE_RETRY = RABBIT_PREFIX+"mlmtech.firststage.retry";
	private static final Integer FIRST_STAGE_RETRY_TTL = 5000;
	private static final Integer FIRST_STAGE_RETRY_CNT = 3;
	
	private static final String QUEUE_SECOND_STAGE_RETRY = RABBIT_PREFIX+"mlmtech.secondstage.retry";
	private static final Integer SECOND_STAGE_RETRY_TTL = 10000;
	private static final Integer SECOND_STAGE_RETRY_CNT = 3;
	
	public static final String EXCHANGE_FROM_M3 = RABBIT_PREFIX+"from.m3";
	
	
	@Bean
	public static HeadersExchange retryExchange() {
		return new HeadersExchange(EXCHANGE_RETRY, true, false);
	}
	
	//////////////////////////////////////////////////// first stage retry config
	
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
				.to(retryExchange())
				.where(RETRY_ARGUMENT)
				.matches(QUEUE_FIRST_STAGE_RETRY);
	}
	
	//////////////////////////////////////////////////// second stage retry config
	
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
				.to(retryExchange())
				.where(RETRY_ARGUMENT)
				.matches(QUEUE_SECOND_STAGE_RETRY);
	}
	
	//////////////////////////////////////////////////////////// NOT PROCESSED ////////////////////////////////////////////
	
	@Bean
	public static TopicExchange notProcessedExchange() {
		return new TopicExchange(EXCHANGE_NOT_PROCESSED, true, false);
	}
	
	@Bean
	public static Binding notProcessedDefaultBinding(TopicExchange notProcessedExchange, Queue notProcessedQueue) {
		return BindingBuilder
				.bind(notProcessedQueue())
				.to(notProcessedExchange())
				.with("#");
	}
	
	@Bean
	public static Queue notProcessedQueue() {
		return new Queue(QUEUE_NOT_PROCESSED, true, false, false);
	}
	
	@Bean
	public static Binding notProcessedBinding(Queue notProcessedQueue) {
		return BindingBuilder
				.bind(notProcessedQueue)
				.to(retryExchange())
				.where(RETRY_ARGUMENT)
				.matches(QUEUE_NOT_PROCESSED);
	}
	
	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	
	@Bean
	public RepublishMessageRecoverer retryRepublishMessageRecoverer(AmqpTemplate amqpTemplate) {
		RepublishMessageRecoverer publisher = new RepublishMessageRecoverer(amqpTemplate, EXCHANGE_RETRY) {
			private static final String MSG_RETRY_COUNT_PARAM = "amqp.retry.count";
			
			@Override
			protected Map<String, Object> additionalHeaders(Message message, Throwable cause) {
				Map addHeaders = super.additionalHeaders(message, cause);
				if(addHeaders == null) {
					addHeaders = new HashMap<>();
				}
				final int cnt = determineAndAddCountHeadParam(addHeaders, message);
				addRetryTargetParam(addHeaders, cnt, message, cause);

				return addHeaders;
			}
			
			private int determineAndAddCountHeadParam(Map<String, Object> addHeaders, Message message) {
				Map<String, Object> headers = message.getMessageProperties().getHeaders();
				Integer cnt = (Integer) headers.get(MSG_RETRY_COUNT_PARAM);
				cnt = cnt == null ? 0 : cnt;
				cnt += 1;
				addHeaders.put(MSG_RETRY_COUNT_PARAM, cnt);
				
				return cnt;
			}
			
			private void addRetryTargetParam(Map<String, Object> addHeaders, final int cnt, Message message, Throwable cause) {
				String targetParam = QUEUE_NOT_PROCESSED;
				if(cnt <= FIRST_STAGE_RETRY_CNT) {
					targetParam = QUEUE_FIRST_STAGE_RETRY;
				} else if(cnt <= FIRST_STAGE_RETRY_CNT + SECOND_STAGE_RETRY_CNT) {
					targetParam = QUEUE_SECOND_STAGE_RETRY;
				}
				
				addHeaders.put(RETRY_ARGUMENT, targetParam);
				
				if(targetParam.equals(QUEUE_NOT_PROCESSED)) {
					String payloadStr = new String(message.getBody(), StandardCharsets.UTF_8);
					logger.error("Message could not be processed after "+cnt+" retries. "+payloadStr, cause);
				} else {
					if(logger.isWarnEnabled()) {
						logger.warn("republish message to "+targetParam);
					}
				}
			}
		};
		publisher.setErrorRoutingKeyPrefix("");
		return publisher;
	}
	
	@Bean
	public RetryOperationsInterceptor retryInterceptor(RepublishMessageRecoverer retryRepublishMessageRecoverer) {
		return RetryInterceptorBuilder.stateless()
				.maxAttempts(1)
				.recoverer(retryRepublishMessageRecoverer)
				.build();
	}
	
	// @formatter:on
}
