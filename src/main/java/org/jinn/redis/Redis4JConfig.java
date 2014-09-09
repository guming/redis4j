package org.jinn.redis;

import redis.clients.jedis.JedisPoolConfig;

public class Redis4JConfig extends JedisPoolConfig{
	int timeInvalidateConnect=0;

	public int getTimeInvalidateConnect() {
		return timeInvalidateConnect;
	}

	public void setTimeInvalidateConnect(int timeInvalidateConnect) {
		this.timeInvalidateConnect = timeInvalidateConnect;
	}
}
