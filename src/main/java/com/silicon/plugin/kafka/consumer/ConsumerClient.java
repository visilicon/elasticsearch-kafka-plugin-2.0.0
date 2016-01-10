package com.silicon.plugin.kafka.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;

import com.silicon.plugin.kafka.Util.Utils;
import com.silicon.plugin.kafka.task.ConsumerTask;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class ConsumerClient {

    private final static ESLogger logger = ESLoggerFactory.getLogger(ConsumerClient.class.getName());
	protected static final String SEP = Utils.LINE_SEPARATOR;
	protected static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
	protected volatile boolean running = false;
	protected Thread thread = null;
	private ExecutorService executor;
	private Client client;
	protected String topics;
	protected Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler() {

		public void uncaughtException(Thread t, Throwable e) {
			logger.error("parse events has an error", e);
		}
	};

	private ConsumerConnector connector = null;
	private Properties properties = null;
	
	public ConsumerClient(String topics, Properties props, Client client) {
		this.topics = topics;
		this.properties = props;
		this.client = client;
	}
	
	private void init(){
		ConsumerConfig config = new ConsumerConfig(this.properties);
		this.connector = Consumer.createJavaConsumerConnector(config);
		this.executor = Executors.newFixedThreadPool(1);
	}

	public void start() throws Exception {
		if(properties == null){
			throw new Exception("## properties is empty");
		}
		this.init();
		if(this.connector == null){
			throw new Exception("## conneciton can't establish for some reason,plz check the config");
		}
		thread = new Thread(new Runnable() {
			public void run() {
				read_from_kafka();
			}
		});

		thread.setUncaughtExceptionHandler(handler);
		thread.start();
		running = true;
	}

	public void stop() {
		if (!running) {
			return;
		}
		running = false;
		if (thread != null) {
			try {
				thread.join();
			} catch (InterruptedException e) {
				// ignore
			}
		}
		if (connector != null) {
			connector.shutdown();
		}
		if (executor != null) {
			executor.shutdown();
		}
	}

	private void read_from_kafka() {
		logger.info("## begin reading from kafka, topic[{}] " + SEP, this.topics);
		String[] allTopics = Strings.splitStringByCommaToArray(topics);
		if(allTopics.length == 0){
			logger.info("## topic interested is empty, return ");
			return;
		}
		Map<String, Integer> topicMap = new HashMap<String, Integer>();
		for(int i = 0; i < allTopics.length; i++){
			String  topic = allTopics[i];
			topicMap.put(topic, Utils.STREAM_NUMBER); //default one stream for each topic;
		}
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = connector.createMessageStreams(topicMap);
		Set<String> keys = consumerMap.keySet();
		for(String key : keys){
			List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(key);
			int threadNum = 0;
			for (KafkaStream<byte[], byte[]> stream : streams) {
				executor.submit(new ConsumerTask(stream, threadNum, key, client));
				threadNum++;
			}
		}
	}
	
	public boolean isRunning(){
		return running;
	}
}
