package org.jinn.redis;

import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;

import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

public abstract class BasePool {
	
	private final GenericObjectPool objectPool;
	
    public abstract String getConnectionName();

	public abstract void initPool() throws JedisException;

	public abstract boolean isAlive();
	
	public BasePool(GenericObjectPool.Config config,PoolableObjectFactory factory) {
		objectPool=new GenericObjectPool(factory,config);
	}
	public Object getResource(){
		try {
			return objectPool.borrowObject();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			 throw new JedisConnectionException((new StringBuilder()).append("Could not get a resource from the pool").toString(), e);
		}
	}
	public void freeResource(Object obj){
		try {
			objectPool.returnObject(obj);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			 throw new JedisConnectionException((new StringBuilder()).append("Could not free a resource from the pool").toString(), e);
		}
	}
	
	public void invalidateResource(Object obj){
		try {
			objectPool.invalidateObject(obj);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new JedisConnectionException((new StringBuilder()).append("Could not invalidate a resource from the pool").toString(), e);
		}
	}
	
	public void freeResource(Object obj,boolean isBroken){
		 if(isBroken)
			 	invalidateResource(obj);
	     else
	    	 freeResource(obj);
	}
	public void destroy(){
		try {
			objectPool.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public int getNumActive(){
		return objectPool.getNumActive();
	}
	public int getNumIdle(){
		return objectPool.getNumIdle();
	}
	public int getMaxActive(){
		return objectPool.getMaxActive();
	}
	
}
