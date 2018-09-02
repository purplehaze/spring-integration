package net.smart4life.springintegration.amqpretry;

import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@EnableScheduling
@Profile("sender")
public class Sender {
	
	private AtomicInteger cnt = new AtomicInteger();
	
//	@Autowired
//	private RabbitItemGateway gateway;
	
	@Autowired
	private RabbitTemplate rabbitTemplate;
	
	@Scheduled(fixedDelay=100, initialDelay=3000)
	public void send() {
		Integer num = cnt.incrementAndGet();
		sendOneMessage(num);
	}
	
//	@EventListener
//	public void handleContextRefresh(ContextRefreshedEvent event) throws Exception {
//		sendOneMessage(3);
//	}
	
	private void sendOneMessage(final Integer num) {
		String msgText = String.format("Dat is message with nr: %d", num);
		log.info("send msg to rabbit: {}", msgText);
		rabbitTemplate.convertAndSend(RetryAndErrorConfig.EXCHANGE_FROM_M3, AmqpConfig.QUEUE_ITEM_CREATED, msgText);
//		gateway.sendToRabbit(msgText);
	}

}
