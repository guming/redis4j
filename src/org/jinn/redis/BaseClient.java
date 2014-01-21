package org.jinn.redis;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

abstract public class BaseClient {
	boolean sanitizeKeys=true;
	public String sanitizeKey( String key ) throws UnsupportedEncodingException {
		return ( sanitizeKeys ) ? URLEncoder.encode( key, "UTF-8" ) : key;
	}
	abstract public Redis4JClient getClient(String key);
}
