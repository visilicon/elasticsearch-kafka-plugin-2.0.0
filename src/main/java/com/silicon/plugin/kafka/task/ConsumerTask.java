package com.silicon.plugin.kafka.task;

import java.io.IOException;
import java.nio.charset.Charset;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.json.JSONObject;

import com.silicon.plugin.kafka.Util.Utils;
import com.silicon.plugin.kafka.listener.DeleteActionListener;
import com.silicon.plugin.kafka.listener.IndexActionListener;
import com.silicon.plugin.kafka.listener.UpdateActionListener;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

public class ConsumerTask implements Runnable {
	private final static ESLogger logger = ESLoggerFactory.getLogger(ConsumerTask.class.getName());

	private int threadNumber;
	private KafkaStream<byte[], byte[]> stream;
	private String topic;
	private Client client;

	public ConsumerTask(KafkaStream<byte[], byte[]> stream, int threadNumber, String topic, Client client) {
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
			String packet = new String(message, Charset.forName("UTF-8"));
			JSONObject json = new JSONObject(packet);
			String type = json.getString(Utils.PREFIX_TYPE);
			String index = json.getString(Utils.PREFIX_INDEX);
			String id = json.getString(Utils.PREFIX_ID);
			String action = json.getString(Utils.PREFIX_ACTION);
			if (action == Utils.ACTION_INSERT) {
				XContentBuilder jsonBuilder = buildRequest(json);
				if (jsonBuilder == null) {
					logger.warn("## insert action json parse error, please check it[{}]", packet);
					continue;
				}
				IndexRequestBuilder builder = client.prepareIndex(index, type, id);
				builder.setSource(jsonBuilder).execute(new IndexActionListener(packet));
			} else if (action == Utils.ACTION_UPDATE) {
				XContentBuilder jsonBuilder = buildRequest(json);
				if (jsonBuilder == null) {
					logger.warn("## delete action json parse error, please check it[{}]", packet);
					continue;
				}
				UpdateRequestBuilder builder = client.prepareUpdate(index, type, id);
				try {
					builder.setSource(jsonBuilder).execute(new UpdateActionListener(packet));
				} catch (Exception e) {
					logger.warn("## update action failed, packet[{}]" + Utils.LINE_SEPARATOR, packet);
				}
			} else if (action == Utils.ACTION_DELETE) {
				client.prepareDelete(index, type, id).execute(new DeleteActionListener(packet));;

			} else {
				logger.warn("## invalid action[{}] for message[{}]", action, packet);
			}
			logger.info("threadNumber[{}], receive message[{}]" + Utils.LINE_SEPARATOR, threadNumber,
					new String(message));
		}

	}

	private XContentBuilder buildRequest(JSONObject json) {
		XContentBuilder jsonBuilder = null;
		try {
			jsonBuilder = XContentFactory.jsonBuilder();
			jsonBuilder.startObject();
			for (String js : json.keySet()) {
				if (js == Utils.PREFIX_ACTION) {
					continue;
				}
				if (js == Utils.PREFIX_INDEX) {
					continue;
				}
				if (js == Utils.PREFIX_ID) {
					continue;
				}
				if (js == Utils.PREFIX_TYPE) {
					continue;
				}
				jsonBuilder.field(js, json.get(js));
			}
			jsonBuilder.endObject();
		} catch (IOException e) {

		} finally {
			jsonBuilder = null;
		}
		return jsonBuilder;
	}

}
