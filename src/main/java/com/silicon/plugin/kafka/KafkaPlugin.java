package com.silicon.plugin.kafka;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestModule;

import com.silicon.plugin.kafka.action.RestKafkaAction;


public class KafkaPlugin extends Plugin{

	@Override
	public String name() {
		return "kafka-plugin";
	}

	@Override
	public String description() {
		return "a plugin for sync kafka and elasticsearch";
	}
	
	public void onModule(RestModule module) {
        module.addRestAction(RestKafkaAction.class);
    }

}
