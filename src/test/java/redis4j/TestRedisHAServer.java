package test.redis4j;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.jinn.redis.Redis4JHAServer;
import org.junit.Test;

import redis.clients.jedis.JedisPoolConfig;


public class TestRedisHAServer {
	@Test
	public void testHAServer(){
		    List servers = new ArrayList();
	        servers.add("10.1.200.80:6379:0");
	        servers.add("10.1.200.81:6379:0");
	        Redis4JHAServer server = new Redis4JHAServer();
	        server.setServers(servers);
	        server.setDoubleWrite(true);
	        server.setPoolConfig(new JedisPoolConfig());
	        Assert.assertEquals("OK", server.set("258.cntrm", "10"));
	        Assert.assertEquals("10", server.get("258.cntrm"));
	        Assert.assertEquals(11,server.incr("258.cntrm").longValue());
	 }
}
