package net.smart4life.springintegration.amqpretry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.retry.RepublishMessageRecoverer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.integration.amqp.dsl.Amqp;
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
	private static final int EXECUTOR_THREAD_COUNT = 10;
	private static final String TRANSFORMED_SUFFIX = "-transformed";
	
	private final List<String> processed = Collections.synchronizedList(new ArrayList<String>());

	@Bean
	public ExecutorService exportFlowsExecutor() {
		return Executors.newFixedThreadPool(EXECUTOR_THREAD_COUNT);
	}
	
//	@Bean
//	public DirectMessageListenerContainer itemMessageListenerContainer(ConnectionFactory connectionFactory, 
//			@Qualifier("itemCreatedQueue") Queue itemCreatedQueue, @Qualifier("itemUpdatedQueue") Queue itemUpdatedQueue) {
//		DirectMessageListenerContainer c = new DirectMessageListenerContainer(connectionFactory);
//		c.addQueues(itemCreatedQueue, itemUpdatedQueue);
//		c.setConsumersPerQueue(EXECUTOR_THREAD_COUNT);
////		c.setTaskExecutor(exportFlowsExecutor());
//		c.setPrefetchCount(1);
//		
//		return c;
//	}
	
	@Bean
	public IntegrationFlow testFlow(ConnectionFactory connectionFactory, 
			@Qualifier("itemCreatedQueue") Queue itemCreatedQueue,
			RetryOperationsInterceptor retryInterceptor
//			,DirectMessageListenerContainer itemMessageListenerContainer
			) {
		return IntegrationFlows
//				.from(itemCreatedMessageProducer)
				.from(Amqp
						.inboundAdapter(connectionFactory, itemCreatedQueue)
						.configureContainer(c->c
								.maxConcurrentConsumers(EXECUTOR_THREAD_COUNT)
								.consecutiveActiveTrigger(2)
								.startConsumerMinInterval(3000)
								.prefetchCount(1)
								.taskExecutor(exportFlowsExecutor())
								.adviceChain(retryInterceptor)
								)
						)
				.headerFilter(RepublishMessageRecoverer.X_EXCEPTION_MESSAGE, RepublishMessageRecoverer.X_EXCEPTION_STACKTRACE, RepublishMessageRecoverer.X_ORIGINAL_EXCHANGE, RepublishMessageRecoverer.X_ORIGINAL_ROUTING_KEY)
				.enrichHeaders(c->c.header("soapAuthHeader", "<soapAuth>someval</soapAuth>"))
				.log(Level.INFO, m->"start processing: "+m)
				.<String, String>transform(p->p+TRANSFORMED_SUFFIX)
				.handle((p, h) -> {
					Integer num = getNum(p);
					if (num % 5 == 0)
						throw new RuntimeException("Boooom in executorThread !!!! Num=" + p + " processedLog=" + processed);
					return p;
				})
				 .handle((p, h) -> {
					 Integer num = getNum(p);
					 if(num % 2 == 0) 
						try {
						 Thread.sleep(5000);
						} catch (Exception e) { };
					 return p;})
				.handle((p, h) -> {
					String txt = (String) p;
					processed.add(txt);
					log.info("processed txt={}", txt);
					return p;
				})
				.log(Level.INFO, m->"end of processing: "+m.getPayload()+ " .......... processedLog=" + processed)
				.get();
	}
	
	private Integer getNum(Object msgPaload) {
		String txt = (String) msgPaload;
		int start = txt.indexOf(": ")+2;
		int end = txt.indexOf(TRANSFORMED_SUFFIX);
		Integer num = Integer.valueOf(end > 0 ? txt.substring(start, end) : txt.substring(start));
		return num;
	}
	
	// @formatter:on
}
