package net.smart4life.springintegration.amqpretry;

import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.ImmediateAcknowledgeAmqpException;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.retry.RepublishMessageRecoverer;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.integration.amqp.support.DefaultAmqpHeaderMapper;
import org.springframework.integration.amqp.support.MappingUtils;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessagingException;

import lombok.extern.slf4j.Slf4j;

/**
 * handler for 'errorChannel' ErrorMessages
 * Decisions has been made here to repost message to one of retry queues or to notProcessed final queue
 * Decisions could be maid by error type or retry count
 * 
 * @author Ilin
 *
 */
@Slf4j
public class ErrorChannelHandlingServiceActivator {
	private static final String AMQP_HEAD_RETRY_COUNT = "msgProcessingRetryCount";
	private static final int MAX_RETRIES_STAGE_1 = 3;
	private static final int MAX_RETRIES_STAGE_2 = 5;
	
	@Autowired
	private RabbitTemplate rabbitTemplate;
	
	@Autowired
	private RepublishMessageRecoverer firstStageRetryMessagePublisher;
	
	@Autowired
	private RepublishMessageRecoverer secondStageRetryMessagePublisher;
	
	@Autowired
	private RepublishMessageRecoverer notProcessedMessagePublisher;

	@ServiceActivator()
	public void handleThrowable(Message<Throwable> errorMessage) throws Throwable {
		Throwable throwable = errorMessage.getPayload();
		org.springframework.amqp.core.Message failedMessage = getFailedMessage(errorMessage);
		if (failedMessage != null) {
			int retryCount = getRetryCount(failedMessage);
			log.info("MSG retry cnt={}", retryCount);
			boolean hasBeenHandled = false;
			hasBeenHandled = decideByErrorType(throwable, failedMessage);
			if(!hasBeenHandled) {
				decideByRetryCount(throwable, failedMessage, retryCount);
			}
		} else {
			throw new ImmediateAcknowledgeAmqpException("can not process message", throwable);
		}
	}

	private boolean decideByErrorType(Throwable throwable, org.springframework.amqp.core.Message failedMessage) {
		boolean hasBeenHandled = false;
		Throwable cause = throwable.getCause();
		if(cause != null) {
			if(cause.getClass() == ImmediateAcknowledgeAmqpException.class) {
				// do nothing simply forget this message
				log.info("Do not process message because of thrown {}. message: {}", ImmediateAcknowledgeAmqpException.class.getSimpleName(), failedMessage);
				hasBeenHandled = true;
			} else if(cause.getClass() == AmqpRejectAndDontRequeueException.class) {
				notProcessedMessagePublisher.recover(failedMessage, throwable);
				hasBeenHandled = true;
			}
		}
		
		return hasBeenHandled;
	}
	
	private boolean decideByRetryCount(Throwable throwable, org.springframework.amqp.core.Message failedMessage,
			int retryCount) {
		if(retryCount < MAX_RETRIES_STAGE_1) {
			increaseRetryCount(failedMessage);
			firstStageRetryMessagePublisher.recover(failedMessage, throwable);
		} else if(retryCount < MAX_RETRIES_STAGE_1 + MAX_RETRIES_STAGE_2) {
			increaseRetryCount(failedMessage);
			secondStageRetryMessagePublisher.recover(failedMessage, throwable);
		} else {
			notProcessedMessagePublisher.recover(failedMessage, throwable);
		}
		
		return true;
	}
	
	private void increaseRetryCount(org.springframework.amqp.core.Message msg){
		int retryCount = getRetryCount(msg);
		retryCount += 1;
		msg.getMessageProperties().getHeaders().put(AMQP_HEAD_RETRY_COUNT, retryCount);
	}
	
	private int getRetryCount(org.springframework.amqp.core.Message message) {
		Integer count = (Integer) message.getMessageProperties().getHeaders().get(ErrorChannelHandlingServiceActivator.AMQP_HEAD_RETRY_COUNT);
		return count == null ? 0 : count;
	}
	
	protected org.springframework.amqp.core.Message getFailedMessage(Message<Throwable> errorMessage){
		Throwable throwable = errorMessage.getPayload();

		if (throwable instanceof MessagingException) {
			Message<?> failedMessage = ((MessagingException) throwable).getFailedMessage();
			MessageConverter converter = this.rabbitTemplate.getMessageConverter();
			return MappingUtils.mapMessage(failedMessage, converter, DefaultAmqpHeaderMapper.outboundMapper(), MessageDeliveryMode.PERSISTENT, true);
		}
		
		return null;
	}
}
