package org.jinn.redis;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool.Config;
import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;


public class Redis4JPool extends BasePool{
	private static final Logger log = Logger.getLogger("jedis");
	
	public static final int CHECK_INTEVAL_TIME = 200;
	public static final int CHECK_TIMEOUT = 500;
	public static final int CHECK_POOL_SIZE = 10;
	
	private static int minConn=1;
	private static int maxConn;
	private AtomicBoolean unhealthy;
	private AtomicInteger backendFails;
	
	private boolean checkStatus;
	private PoolableObjectFactory factory;
    private String host;
    private int backendFaisThreadhole;
    private Thread checkStatusThread;
    private Thread invalidateThread;
    private boolean evictor;
	
    private String connectionName;
    
	public Redis4JPool(Config config, PoolableObjectFactory factory) {
		super(config, factory);
		// TODO Auto-generated constructor stub
		minConn=config.minIdle;
		maxConn=config.maxActive;
		checkStatus=true;
		evictor=true;
		this.factory = factory;
		
		if(factory instanceof Redis4JFactory)
			host=((Redis4JFactory)factory).getFactoryName();
		else
			host=factory.toString();
		
		unhealthy=new AtomicBoolean(false);
		backendFails=new AtomicInteger(0);
		backendFaisThreadhole=config.minIdle;
		
		evictor=config.testWhileIdle;
		setCheckStatus(evictor);
		setConnectionName((new StringBuilder()).append("Conn<").append(factory.toString()).append(">").toString());
		if(config instanceof Redis4JConfig){
			  int time = ((Redis4JConfig)config).getTimeInvalidateConnect();
	          if(time > 0){
	                invalidateThread = new InvalidateThread(time);
	                invalidateThread.setDaemon(true);
	                invalidateThread.start();
	          }
		}
	}
	
	public static GenericObjectPool.Config newCustomPoolConfig(int minConnCount, int maxConnCount, boolean enableEvictor){
        org.apache.commons.pool.impl.GenericObjectPool.Config poolConfig = new org.apache.commons.pool.impl.GenericObjectPool.Config();
        poolConfig.maxActive = maxConnCount;
        poolConfig.maxIdle = minConnCount;
        poolConfig.minIdle = minConnCount;
        if(enableEvictor){
            poolConfig.numTestsPerEvictionRun = -4;//每次清理对象的1/4
            poolConfig.timeBetweenEvictionRunsMillis = 30000L;//间隔30000L 进行一次后台对象清理的行动
            poolConfig.testWhileIdle = true;//设定在进行后台对象清理时，是否还对没有过期的池内对象进行有效性检查。不能通过有效性检查的对象也将被回收
            poolConfig.minEvictableIdleTimeMillis = -1L;//设定在进行后台对象清理时，视休眠时间超过了多少毫秒的对象为过期。过期的对象将被回收。如果这个值不是正数，那么对休眠时间没有特别的约束
            poolConfig.softMinEvictableIdleTimeMillis = 0x36ee80L;//空闲数大于minIdle 并且休眠时间超过了0x36ee80L毫秒的对象为过期。过期的对象将被回收
        }
        poolConfig.maxWait = 500L;//指明若在对象池空时调用borrowObject方法的行为被设定成等待，最多等待多少毫秒。如果等待时间超过了这个数值，则会抛出一个java.util.NoSuchElementException异常。如果这个值不是正数，表示无限期等待
        poolConfig.whenExhaustedAction = 1;//0,1,2;1表示GenericObjectPool.WHEN_EXHAUSTED_BLOCK 等待;指定在池中借出对象的数目已达极限的情况下，调用它的borrowObject方法时的行为.
        return poolConfig;
    }
	
    public Redis4JPool(String host, int port, String password, int database, int minConnCount, int maxConnCount)
    {
        this(host, port, password, database, minConnCount, maxConnCount, true);
    }

    public Redis4JPool(String host, int port, String password, int database, int minConnCount, int maxConnCount, boolean enableEvictor)
    {
        this(newCustomPoolConfig(minConnCount, maxConnCount, enableEvictor), ((PoolableObjectFactory) (new Redis4JFactory(host, port, 2000, password, database))));
    }

	
	private class InvalidateThread extends Thread{
	 	private int sTime;
		   
        public InvalidateThread( int sTime) {
			super();
			this.sTime = sTime;
		}

		public void run(){  
        	try {
    		doInvalidateConnect();
            Thread.sleep(sTime);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
        }  
	}
	
	public void doInvalidateConnect(){
		if(unhealthy==null||!unhealthy.get()){
			Jedis client=(Jedis)super.getResource();
		     super.invalidateResource(client);
	         log.info((new StringBuilder()).append("invalidate client success: ").append(client).toString());
		}
	}
	
	
	
	@Override
	public String getConnectionName() {
		return this.connectionName;
	}

	private void setConnectionName(String name) {
		 this.connectionName = name;
	}
	
	@Override
	public void initPool() throws JedisException {
		// TODO Auto-generated method stub
		 log.info((new StringBuilder()).append("initPool: ").append(toString()));
		if(unhealthy!=null){
			unhealthy.compareAndSet(true, false);
		}
	     List<Jedis> conns = new ArrayList<Jedis>();
	    try {
	    	for (int i = 0; i < minConn; i++) {
	    		Jedis client = (Jedis)super.getResource();
	    			conns.add(client);
	    	}
		} catch (Exception e) {
			// TODO: handle exception
	      log.error("init conn pool failed:" + getConnectionName() + e);
	      if (this.unhealthy != null)
	        this.unhealthy.compareAndSet(false, true);
		}
		try{
		    for (Jedis client : conns)
		        super.freeResource(client);
		    }
		    catch (Exception e) {
		      log.error("init conn pool failed:" + getConnectionName() + e);
		      if (this.unhealthy != null)
		        this.unhealthy.compareAndSet(false, true);
		    }
		
	}

	@Override
	public boolean isAlive() {
		// TODO Auto-generated method stub
		return unhealthy==null||!unhealthy.get();
	}

	public boolean isCheckStatus() {
		return checkStatus;
	}

	public void setCheckStatus(boolean checkStatus) {
		this.checkStatus = checkStatus;
		if(!this.checkStatus){
			 if (this.checkStatusThread != null) {
		          this.checkStatusThread.interrupt();
		          this.checkStatusThread = null;
		        }
		        if (this.unhealthy != null) {
		          this.unhealthy.compareAndSet(true, false);
		        }
		        return;
		}
		if ((this.checkStatus) && 
			        (this.checkStatusThread == null)){
			checkStatusThread=new Thread(this.connectionName + "-check"){
				  public void run() {
					  while(true){
						  if(Redis4JPool.this.checkStatus){
							  Redis4JPool.this.checkJedisStatus();
							  try {
								  Thread.sleep(200L);
							} catch (Exception e) {
								// TODO: handle exception
							}
						  }
					  }
				  }
			};
		}
	   this.checkStatusThread.setDaemon(true);
	   this.checkStatusThread.start();
		
	}
	private void checkJedisStatus() {
		 if (isIgnore()) {
	          this.unhealthy.compareAndSet(false, true);
	          return;
	     }
		 if(unhealthy!=null&&unhealthy.get()){
		    Jedis client = null;
			boolean isBad = false;
			try {
				 client=(Jedis)super.getResource();
				 client.ping();
			     this.unhealthy.compareAndSet(true, false);
		         this.backendFails.set(0);
		         log.info("detect backend recover from failure,set unhealthy=false " + getConnectionName());
		         
			} catch (Exception e) {
				// TODO: handle exception
				isBad = true;
			}finally{
			  if (client != null){
				if(isBad){
					super.invalidateResource(client);
				}else{
					super.freeResource(client);
				}
			  }
			}
			 
		 }
	}
	
	public Jedis getResource(){
        if(checkStatus && unhealthy != null && unhealthy.get())
            throw new JedisConnectionException((new StringBuilder()).append("status check fail for ").append(getConnectionName()).toString());
        else
            return (Jedis)super.getResource();
    }

    public Jedis tryGetResource(){
        return (Jedis)super.getResource();
    }

    public void returnBrokenResource(Jedis resource){
        super.invalidateResource(resource);
        if(checkStatus && unhealthy != null && !unhealthy.get())
        {
        	//过多的销毁被认为不健康
            int fails = backendFails.incrementAndGet();
            if(fails > backendFaisThreadhole)
                unhealthy.compareAndSet(false, true);
        }
    }

    public void returnResource(Jedis resource){
        super.freeResource(resource);
        //return成功说明健康
        if(checkStatus){
            backendFails.set(0);
        }
    }
	public int getBackendFaisThreadhole() {
		return backendFaisThreadhole;
	}

	public void setBackendFaisThreadhole(int backendFaisThreadhole) {
		this.backendFaisThreadhole = backendFaisThreadhole;
	}

	public String toString()
    {
        return factory.toString();
    }
    private boolean isIgnore()
    {
        return false;
    }
}
