package com.silicon.plugin.kafka.Util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.Properties;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;

import com.silicon.plugin.kafka.KafkaPlugin;

public class Utils {
    private final static ESLogger logger = ESLoggerFactory.getLogger(Utils.class.getName());

	
	public static String LINE_SEPARATOR = "\r\n";
	public static Integer STREAM_NUMBER = 1;

	public static String PREFIX_INDEX = "_index";
	public static String PREFIX_TYPE = "_type";
	public static String PREFIX_ID = "_id";
	public static String PREFIX_ACTION = "_action";

	public static String ACTION_INSERT = "insert";
	public static String ACTION_UPDATE = "update";
	public static String ACTION_DELETE = "delete";

	public static Properties properties = null;

	public static Properties loadConf() {
		if (properties == null) {
			InputStream stream = KafkaPlugin.class.getClassLoader().getResourceAsStream("es-plugin.properties");
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			byte[] buffer = new byte[2048];
			int len;
			try {
				while ((len = stream.read(buffer)) != -1) {
					out.write(buffer, 0, len);
				}
				stream.close();
				properties = new Properties();
				properties.load(new StringReader(new String(out.toByteArray())));
			} catch (IOException e) {
				logger.warn("## load kafka-plugin error");
			}
		}
		return properties;
	}
}
