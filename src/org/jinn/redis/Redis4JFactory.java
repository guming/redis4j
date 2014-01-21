package org.jinn.redis;

import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;


public class Redis4JFactory extends BasePoolableObjectFactory {
	
	private static final Logger log = Logger.getLogger("jedis");
    private final String host;
    private final int port;
    private final int timeout;
    private final String password;
    private final int dbName;
    public static final int DEFAUTL_TIMEOUT = 500;
    public static final String DEFAULT_PASSWORD = null;
    public static final int DEFAULT_DBNAME = 0;//default=0
    public Redis4JFactory(String serverPort)
    {
        this(serverPort, 0, DEFAUTL_TIMEOUT, DEFAULT_PASSWORD, DEFAULT_DBNAME);
    }

    public Redis4JFactory(String host, int port)
    {
        this(host, port, DEFAUTL_TIMEOUT, DEFAULT_PASSWORD, DEFAULT_DBNAME);
    }
	public Redis4JFactory(String host, int port, int timeout, String password,
		int dbName) {
		if(port>0){
			this.host = host;
			this.port = port;
			this.dbName = dbName;
		}else{
			String parts[] = host.split(":");
			if(parts.length>0){
				this.host = parts[0];
			    this.port = Integer.parseInt(parts[1]);
		        if(parts.length > 2){
		                this.dbName = Integer.parseInt(parts[2]);
		        }else{
		                this.dbName = dbName;
		        }
			}else{
				this.host = host;
				this.port = port;
				this.dbName = dbName;
			}
		}
		this.password = password;
		this.timeout = timeout;
	}

	@Override
	public Object makeObject() throws Exception {
		// TODO Auto-generated method stub
		 Jedis jedis;
		 if(timeout>0){
			 jedis=new Jedis(host,port,timeout);
		 }else{
			 jedis=new Jedis(host,port);
		 }
		 jedis.connect();
		 jedis.select(dbName);
		 if(password!=null&&!password.equals("")){
			 jedis.auth(password);
		 }
		return jedis;
	}
	
	public void destoryObeject(Object obj)throws Exception{
		if(obj instanceof Jedis){
			Jedis jedis = (Jedis)obj;
			try {
				try {
					jedis.quit();
				} catch (Exception e) {
					log.warn((new StringBuilder()).append("jedis quit error: ").append(jedis).append(" ").append(e.getMessage()).toString());
				}
				jedis.disconnect();
			} catch (Exception e) {
				log.warn((new StringBuilder()).append("jedis disconnect error: ").append(jedis).append(" ").append(e.getMessage()).toString());
			}
		}
	}
	
	public boolean validateObject(Object obj){
		  if(obj instanceof Jedis)
	        {
	            Jedis jedis = (Jedis)obj;
	            try
	            {
	                return jedis.isConnected() && jedis.ping().equals("PONG");
	            }
	            catch(Exception e)
	            {
	                return false;
	            }
	        } else
	        {
	            return false;
	        }
	}
	
	public String getFactoryName(){
	        if(dbName > 0)
	            return (new StringBuilder()).append(host).append(":").append(port).append(":").append(dbName).toString();
	        else
	            return (new StringBuilder()).append(host).append(":").append(port).toString();
	}

    public String toString()
    {
        return (new StringBuilder()).append("REDIS ").append(host).append(":").append(port).append(":").append(dbName).toString();
    }
}
