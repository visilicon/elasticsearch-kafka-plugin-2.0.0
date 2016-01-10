package com.silicon.plugin.kafka.listener;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;

import com.silicon.plugin.kafka.Util.Utils;

public class IndexActionListener implements ActionListener<IndexResponse> {
    private final static ESLogger logger = ESLoggerFactory.getLogger(IndexActionListener.class.getName());

	private String message;
	public IndexActionListener(String message){
		this.message = message;
	}
	
	@Override
	public void onFailure(Throwable e) {
		logger.warn("## index action failed for message[{}]" + Utils.LINE_SEPARATOR, e, message);
	}

	@Override
	public void onResponse(IndexResponse response) {
		//nothing
	}

}
