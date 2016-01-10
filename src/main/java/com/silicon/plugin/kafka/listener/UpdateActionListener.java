package com.silicon.plugin.kafka.listener;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;

import com.silicon.plugin.kafka.Util.Utils;

public class UpdateActionListener implements ActionListener<UpdateResponse> {
    private final static ESLogger logger = ESLoggerFactory.getLogger(UpdateActionListener.class.getName());

	private String message;
	public UpdateActionListener(String message){
		this.message = message;
	}
	
	@Override
	public void onFailure(Throwable e) {
		logger.warn("## update action failed for message[{}]" + Utils.LINE_SEPARATOR, e, message);
	}

	@Override
	public void onResponse(UpdateResponse response) {
		//nothing
	}

}
