package net.smart4life.springintegration.amqpretry;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.config.RetryInterceptorBuilder;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.retry.RepublishMessageRecoverer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.amqp.inbound.AmqpInboundChannelAdapter;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.handler.LoggingHandler.Level;
import org.springframework.retry.interceptor.RetryOperationsInterceptor;

import lombok.extern.slf4j.Slf4j;

@Configuration
@Slf4j
@Profile("receiver")
public class IntegrationConfig {
	// @formatter:off
	private static final int MAX_RETRY_ATTEMPTS_IN_MAIN_THREAD = 24 * 4 + 10;
	
	private final List<String> processed = new ArrayList<>();

	@Bean
	public ExecutorService exportFlowsExecutor() {
		return Executors.newFixedThreadPool(3);
	}

	@Bean
	RetryOperationsInterceptor retryInterceptor(AmqpTemplate amqpTemplate) {
		return RetryInterceptorBuilder.stateless()
				.maxAttempts(MAX_RETRY_ATTEMPTS_IN_MAIN_THREAD)
				.backOffOptions(1000, 2, 900000) // start with 1000ms, multiply by 2, go until 900000ms
				.recoverer(new RepublishMessageRecoverer(amqpTemplate, RetryAndErrorConfig.EXCHANGE_NOT_PROCESSED) {
					@Override
					public void recover(Message message, Throwable cause) {
						super.recover(message, cause);
						log.error("could not process message: {}", message);
					}
				})
				.build();
	}
	
	@Bean
	public AmqpInboundChannelAdapter itemCreatedMessageProducer(
			ConnectionFactory connectionFactory, 
			@Qualifier("itemCreatedQueue") Queue itemCreatedQueue, 
			RetryOperationsInterceptor retryInterceptor) {// NOSONAR
		return Amqp
				.inboundAdapter(connectionFactory, itemCreatedQueue)
//				.errorChannel(MessageHeaders.ERROR_CHANNEL)
				.configureContainer(c->c
						.adviceChain(retryInterceptor)
//						.taskExecutor(exportFlowsExecutor())
						)
//				.configureContainer(c->c.adviceChain(new AbstractMessageSourceAdvice() {
//					@Override
//					public boolean beforeReceive(MessageSource<?> source) {
//						log.info("!!!!!!!!!!! beforeReceive({})", source);
//						return true;
//					}
//
//					@Override
//					public org.springframework.messaging.Message<?> afterReceive(org.springframework.messaging.Message<?> result, MessageSource<?> source) {
//						log.info("!!!!!!!!!!! afterReceive({}, {})", result, source);
//						return result;
//					}
//
//				}))
				.get()
				;
	}
	
	@Bean
	public IntegrationFlow testFlow(AmqpInboundChannelAdapter itemCreatedMessageProducer) {
		return IntegrationFlows
				.from(itemCreatedMessageProducer)
				.log(Level.INFO)
				.channel(c -> c.executor(exportFlowsExecutor()))
				.headerFilter(RepublishMessageRecoverer.X_EXCEPTION_STACKTRACE)
//				.handle((p, h) -> { Integer num = getNum(p); if(num % 3 == 0) throw new AmqpRejectAndDontRequeueException("Boooom!!!! Num="+p); return p;})
//				.handle((p, h) -> { Integer num = getNum(p); if(num % 3 == 0) throw new ImmediateAcknowledgeAmqpException("Boooom!!!! Num="+p); return p;})
//				.handle((p, h) -> {
//					Integer num = getNum(p);
//					if (num % 3 == 0)
//						throw new RuntimeException("Boooom in executorThread !!!! Num=" + p + " processedLog=" + processed);
//					return p;
//				})
				 .handle((p, h) -> {
					 Integer num = getNum(p);
					 if(num % 3 == 0) 
						try {
						 Thread.sleep(10000);
						} catch (Exception e) { };
					 return p;})
				.handle((p, h) -> {
					String txt = (String) p;
					processed.add(txt);
					log.info("processed txt={}", txt);
					return p;
				})
				.log(Level.INFO, p->"end of processing "+p)
				.get();
	}
	
	private Integer getNum(Object msgPaload) {
		String txt = (String) msgPaload;
		Integer num = Integer.valueOf(txt.substring(txt.indexOf(": ")+2));
		return num;
	}
	
	// @formatter:on
}
