package com.silicon.plugin.kafka.task;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;

import com.silicon.plugin.kafka.Util.Utils;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

public class SimpleTask implements Runnable {
	private final static ESLogger logger = ESLoggerFactory.getLogger(SimpleTask.class.getName());

	private int threadNumber;
	private KafkaStream<byte[], byte[]> stream;
	private String topic;
	private Client client;

	public SimpleTask(KafkaStream<byte[], byte[]> stream, int threadNumber, String topic, Client client) {
		this.threadNumber = threadNumber;
		this.stream = stream;
		this.topic = topic;
		this.client = client;
	}

	@Override
	public void run() {
		logger.info("## run topic[{}] in threadNumber[{}]" + Utils.LINE_SEPARATOR, topic, threadNumber);
		ConsumerIterator<byte[], byte[]> it = stream.iterator();
		while (it.hasNext()) {
			MessageAndMetadata<byte[], byte[]> meta = it.next();
			byte[] message = meta.message();
			logger.info("threadNumber[{}], receive message[{}],client[{}]" + Utils.LINE_SEPARATOR, threadNumber,
					new String(message), client.toString());
		}

	}

}
