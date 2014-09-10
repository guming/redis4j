package redis4j;

import java.util.ArrayList;
import java.util.List;

import org.jinn.redis.Redis4JHAServer;

import redis.clients.jedis.JedisPoolConfig;


public class TestRedis4J {
	static final Integer b=1;
	/**
	 * @param args
	 */
	 public static void main(String args[]){
		    List servers = new ArrayList();
	        servers.add("127.0.0.1:6379:0");
	        servers.add("127.0.0.1:6379:0");
	        Redis4JHAServer server = new Redis4JHAServer();
	        server.setServers(servers);
	        server.setDoubleWrite(true);
	        server.setPoolConfig(new JedisPoolConfig());
	        System.out.println(server.set("258.cntrm", "10"));
	        System.out.println(server.get("258.cntrm"));
	        System.out.println(server.incr("258.cntrm"));
//	        System.out.println(server.get("258.cntrm"));
//     		Integer a=Integer.valueOf(1);
//			System.out.println("Test:"+(a==b));
	 }
}
