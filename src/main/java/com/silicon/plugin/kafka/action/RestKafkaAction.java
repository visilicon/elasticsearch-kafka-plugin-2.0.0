package com.silicon.plugin.kafka.action;

import java.util.Properties;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import com.silicon.plugin.kafka.Util.Errno;
import com.silicon.plugin.kafka.Util.Utils;
import com.silicon.plugin.kafka.consumer.ConsumerClient;

public class RestKafkaAction extends BaseRestHandler {
	private final static ESLogger logger = ESLoggerFactory.getLogger(RestKafkaAction.class.getName());

	private int errno;

	protected RestKafkaAction(Settings settings, RestController controller, Client client) {
		super(settings, controller, client);
		controller.registerHandler(RestRequest.Method.GET, "/_kafka/start", this);
		controller.registerHandler(RestRequest.Method.POST, "/_kafka/start", this);
		controller.registerHandler(RestRequest.Method.GET, "/_kafka/stop", this);
		controller.registerHandler(RestRequest.Method.POST, "/_kafka/stop", this);
		this.errno = Errno.SUCCESS;
	}

	@Override
	protected void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception {
		String action = null;
		String path = request.path();
		if (path.endsWith("start")) {
			action = "start";
		} else if (path.endsWith("stop")) {
			action = "stop";
		}
		do {
			if (action == null) {
				errno = Errno.ERRNO_INVALID_ACTION;
				logger.warn("## invalid url[{}]", path);
				break;
			}
			Properties props = Utils.loadConf();
			String topic = props.getProperty("kafka.topic");
			ConsumerClient consumer = new ConsumerClient(topic, props, client);
			if (consumer.isRunning() && action == "start") {
				errno = Errno.ERRNO_ISRUNNING;
				break;
			}
			if (!consumer.isRunning() && action == "stop") {
				errno = Errno.SUCCESS;
				break;
			}

			if (action == "start") {
				consumer.start();
			} else {
				consumer.stop();
			}

		} while (false);
		XContentBuilder jsonBuilder = null;
		jsonBuilder = XContentFactory.jsonBuilder();
		jsonBuilder.startObject();
		jsonBuilder.field("errno", errno);
		jsonBuilder.field("errmsg", Errno.getErrmsg(errno));
		jsonBuilder.endObject();
		BytesRestResponse response = new BytesRestResponse(RestStatus.OK, jsonBuilder.string());
		channel.sendResponse(response);

	}

}
