package com.silicon.plugin.kafka.listener;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;

import com.silicon.plugin.kafka.Util.Utils;

public class DeleteActionListener implements ActionListener<DeleteResponse> {
    private final static ESLogger logger = ESLoggerFactory.getLogger(DeleteActionListener.class.getName());

	private String message;
	public DeleteActionListener(String message){
		this.message = message;
	}
	
	@Override
	public void onFailure(Throwable e) {
		logger.warn("## delete action failed for message[{}]" + Utils.LINE_SEPARATOR, e, message);
	}

	@Override
	public void onResponse(DeleteResponse response) {
		//nothing
	}

}
