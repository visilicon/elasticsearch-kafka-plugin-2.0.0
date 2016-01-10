package com.silicon.plugin.kafka.Util;

import java.util.HashMap;
import java.util.Map;

public class Errno {
	
	public static int SUCCESS = 0;
	public static int ERRNO_INVALID_ACTION = 1;
	public static int ERRNO_ISRUNNING = 2;
	public static int ERRNO_UNRECOGNIZED = 99;
	
	public static Map<Integer, String> errmap = new HashMap<Integer, String>();
	
	static {
		errmap.put(new Integer(SUCCESS), "success");
		errmap.put(new Integer(ERRNO_INVALID_ACTION), "parameter unsupported");
	}
	
	public static String getErrmsg(int errno){
		String errmsg = errmap.get(new Integer(errno));
		if(errmsg == null){
			errmsg = "action unrecognized";
		}
		return errmsg;
	}
	
}
