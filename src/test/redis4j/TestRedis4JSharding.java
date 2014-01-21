package test.redis4j;

import java.util.ArrayList;
import java.util.List;

import org.jinn.redis.Redis4JConfig;
import org.jinn.redis.Redis4JShardingServer;

public class TestRedis4JSharding {
	public static void main(String[] args) {
		 List servers = new ArrayList();
	        servers.add("10.1.200.80:6379:0");
	        servers.add("10.1.200.81:6379:0");
	        servers.add("10.1.200.82:6379:0");
		Redis4JShardingServer server=new Redis4JShardingServer();
		  server.setServers(servers);
		  Redis4JConfig rconf= new Redis4JConfig();
		  rconf.setMaxIdle(5);
		  rconf.setMinIdle(2);
		  server.setPoolConfig(rconf);
		  server.start();
//		 for (int i = 2000; i < 3000; i++) {
//			 try{
//			 server.set(i+"gm:test:", "myname"+i);
//			 }catch(Exception e){
//				 
//			 }
//		 }
		 int count1=0;
		 for (int i = 2000; i < 3000; i++) {
			 try{
				if(server.get(i+"gm:test:")!=null)
				 count1++;
				
			 }catch(Exception e){
				 
			 }
		 }
		 System.out.println( "read count1:"+count1);
	}
}
