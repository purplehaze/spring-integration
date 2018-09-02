package net.smart4life.springintegration.amqpretry;

public interface AmqpConstants {
	public static final String DLX_HEAD = "x-dead-letter-exchange";
	public static final String DLX_TTL_HEAD = "x-message-ttl";
}
