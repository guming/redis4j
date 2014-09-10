package redis4j;

import junit.framework.Assert;

import org.jinn.redis.Redis4JMSServer;
import org.junit.Test;

import redis.clients.jedis.JedisPoolConfig;

public class TestRedisMSServer {
		@Test
		public void testServer(){
		        Redis4JMSServer server = new Redis4JMSServer();
		        server.setPoolConfig(new JedisPoolConfig());
		        server.setMasterServer("10.1.200.80:6379");
		        server.setSlaveServer("10.1.200.81:6379");
		        server.start();
		        Assert.assertEquals("OK", server.set("258.cntrm", "10"));
		        Assert.assertEquals(11,server.incr("258.cntrm").longValue());
		}
}
