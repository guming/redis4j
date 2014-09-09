package org.jinn.redis;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.pool.impl.GenericObjectPool.Config;
import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;

public class Redis4JClient extends BaseClient implements Redis4JCommand {
	private static final String DEFAULT_SERVER_KEY="default";
	protected static Logger log = Logger.getLogger("jedis");
	protected static final String REDIS_RET_OK = "OK";
	protected static final int REDIS_SLOW_TIME = 50;
	protected org.apache.commons.pool.impl.GenericObjectPool.Config poolConfig;
	protected BasePool conn;
	protected String server;
	protected boolean throwJedisException;
	protected boolean readOnly;
	protected long hashMin;
	protected long hashMax;
	public static final Double readOnlyDefaultDoubleValue = Double.valueOf(-1L);
	public static final Long readOnlyDefaultLongValue = Long.valueOf(-1L);
	public static final Boolean readOnlyDefaultBooleanValue = Boolean.valueOf(false);
	
	public Redis4JClient(){
	        throwJedisException = false;
	        readOnly = false;
	        hashMin = -1L;
	        hashMax = -1L;
	}
	public Redis4JClient(boolean readOnly, long hashMin, long hashMax) {
		this();
		this.readOnly = readOnly;
		this.hashMin = hashMin;
		this.hashMax = hashMax;
	}

	public Redis4JClient(Config poolConfig, BasePool conn, long hashMin,
			long hashMax) {
		this();
		this.poolConfig = poolConfig;
		this.conn = conn;
		this.hashMin = hashMin;
		this.hashMax = hashMax;
	}
	
	
	
	@Override
	public boolean isAlive() {
		return conn.isAlive();
	}

    public void close()
    {
        conn.destroy();
    }
	
	@Override
	public String flushDB() {
		Jedis jedis=null;
		String value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)getJedis(this);
			value=jedis.flushDB();
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public String ping() {
		Jedis jedis=null;
		String value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)getJedis(this);
			value=jedis.ping();
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public String set(String s, String s1) {
		if(readOnly)
            return null;
		Jedis jedis=null;
		String value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.set(s,s1);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public String get(String s) {
		// TODO Auto-generated method stub
		Jedis jedis=null;
		String value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.get(s);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Boolean exists(String s) {
		// TODO Auto-generated method stub
		Jedis jedis=null;
		boolean value=false;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.exists(s);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public String type(String s) {
		// TODO Auto-generated method stub
		Jedis jedis=null;
		String value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.type(s);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Long expire(String s, int i) {
		// TODO Auto-generated method stub
		Jedis jedis=null;
		Long value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.expire(s,i);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Long expireAt(String s, long l) {
		// TODO Auto-generated method stub
		Jedis jedis=null;
		Long value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.expireAt(s,l);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Long ttl(String s) {
		// TODO Auto-generated method stub
		Jedis jedis=null;
		Long value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.ttl(s);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Long setrange(String s, long l, String s1) {
		// TODO Auto-generated method stub
		if(readOnly)
			 return readOnlyDefaultLongValue;
		Jedis jedis=null;
		Long value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.setrange(s,l,s1);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public String getrange(String s, long l, long l1) {
		// TODO Auto-generated method stub
		Jedis jedis=null;
		String value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.getrange(s,l,l1);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public String getSet(String s, String s1) {
		// TODO Auto-generated method stub
		Jedis jedis=null;
		String value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.getSet(s,s1);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Long setnx(String s, String s1) {
		// TODO Auto-generated method stub
		if(readOnly)
			 return readOnlyDefaultLongValue;
		Jedis jedis=null;
		Long value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.setnx(s,s1);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public String setex(String s, int i, String s1) {
		// TODO Auto-generated method stub
		if(readOnly)
			 return null;
		Jedis jedis=null;
		String value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.setex(s,i,s1);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Long decrBy(String s, long l) {
		// TODO Auto-generated method stub
		if(readOnly)
			 return readOnlyDefaultLongValue;
		Jedis jedis=null;
		Long value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.decrBy(s,l);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Long decr(String s) {
		// TODO Auto-generated method stub
		if(readOnly)
			 return readOnlyDefaultLongValue;
		Jedis jedis=null;
		Long value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.decr(s);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Long incrBy(String s, long l) {
		// TODO Auto-generated method stub
		if(readOnly)
			 return readOnlyDefaultLongValue;
		Jedis jedis=null;
		Long value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.incrBy(s,l);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Long incr(String s) {
		// TODO Auto-generated method stub
		if(readOnly)
			 return readOnlyDefaultLongValue;
		Jedis jedis=null;
		Long value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.incr(s);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Long append(String s, String s1) {
		// TODO Auto-generated method stub
		if(readOnly)
			 return readOnlyDefaultLongValue;
		Jedis jedis=null;
		Long value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.append(s,s1);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public String substr(String s, int i, int j) {
		// TODO Auto-generated method stub
		if(readOnly)
			 return null;
		Jedis jedis=null;
		String value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.substr(s,i,j);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Long hset(String s, String s1, String s2) {
		// TODO Auto-generated method stub
		if(readOnly)
			 return readOnlyDefaultLongValue;
		Jedis jedis=null;
		Long value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.hset(s,s1,s2);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public String hget(String s, String s1) {
		// TODO Auto-generated method stub
		Jedis jedis=null;
		String value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.hget(s,s1);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Long hsetnx(String s, String s1, String s2) {
		// TODO Auto-generated method stub
		if(readOnly)
			 return readOnlyDefaultLongValue;
		Jedis jedis=null;
		Long value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.hsetnx(s,s1,s2);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public String hmset(String s, Map map) {
		// TODO Auto-generated method stub
		if(readOnly)
			 return null;
		Jedis jedis=null;
		String value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.hmset(s,map);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public List hmget(String s, String... as) {
		// TODO Auto-generated method stub
		Jedis jedis=null;
		List value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.hmget(s,as);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Long hincrBy(String s, String s1, long l) {
		// TODO Auto-generated method stub
		if(readOnly)
			 return readOnlyDefaultLongValue;
		Jedis jedis=null;
		Long value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.hincrBy(s,s1,l);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Boolean hexists(String s, String s1) {
		// TODO Auto-generated method stub
		Jedis jedis=null;
		Boolean value=false;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.hexists(s,s1);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Long hdel(String s, String... as) {
		// TODO Auto-generated method stub
		if(readOnly)
			 return readOnlyDefaultLongValue;
		Jedis jedis=null;
		Long value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.hdel(s,as);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Long hlen(String s) {
		// TODO Auto-generated method stub
		Jedis jedis=null;
		Long value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.hlen(s);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Set hkeys(String s) {
		// TODO Auto-generated method stub
		Jedis jedis=null;
		Set value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.hkeys(s);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public List hvals(String s) {
		Jedis jedis=null;
		List value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.hvals(s);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Map hgetAll(String s) {
		Jedis jedis=null;
		Map value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.hgetAll(s);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Long rpush(String s, String... as) {
		if(readOnly)
			 return readOnlyDefaultLongValue;
		Jedis jedis=null;
		Long value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.rpush(s,as);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Long lpush(String s, String... as) {
		if(readOnly)
			 return readOnlyDefaultLongValue;
		Jedis jedis=null;
		Long value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.lpush(s,as);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Long llen(String s) {
		Jedis jedis=null;
		Long value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.llen(s);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public List lrange(String s, long l, long l1) {
		Jedis jedis=null;
		List value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.lrange(s,l,l1);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public String ltrim(String s, long l, long l1) {
		Jedis jedis=null;
		String value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.ltrim(s,l,l1);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public String lindex(String s, long l) {
		Jedis jedis=null;
		String value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.lindex(s,l);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public String lset(String s, long l, String s1) {
		if(readOnly)
			 return null;
		Jedis jedis=null;
		String value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.lset(s,l,s1);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Long lrem(String s, long l, String s1) {
		   if(readOnly)
	            return readOnlyDefaultLongValue;
		Jedis jedis=null;
		Long value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.lrem(s,l,s1);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public String lpop(String s) {
		if(readOnly)
	            return null;
		Jedis jedis=null;
		String value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.lpop(s);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public String rpop(String s) {
		   if(readOnly)
	            return null;
		Jedis jedis=null;
		String value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.rpop(s);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Long sadd(String s, String... as) {
		   if(readOnly)
	            return readOnlyDefaultLongValue;
		Jedis jedis=null;
		Long value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.sadd(s,as);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Set smembers(String s) {
		Jedis jedis=null;
		Set value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.smembers(s);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Long srem(String s, String... as) {
		   if(readOnly)
	            return readOnlyDefaultLongValue;
		Jedis jedis=null;
		Long value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.srem(s,as);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public String spop(String s) {
		   if(readOnly)
	            return null;
		Jedis jedis=null;
		String value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.spop(s);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Long scard(String s) {
		Jedis jedis=null;
		Long value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.scard(s);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Boolean sismember(String s, String s1) {
		Jedis jedis=null;
		Boolean value=false;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.sismember(s,s1);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public String srandmember(String s) {
		Jedis jedis=null;
		String value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.srandmember(s);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Long zadd(String s, double d, String s1) {
		   if(readOnly)
	            return readOnlyDefaultLongValue;
		Jedis jedis=null;
		Long value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.zadd(s,d,s1);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Long zadd(String s, Map map) {
		   if(readOnly)
	            return readOnlyDefaultLongValue;
		Jedis jedis=null;
		Long value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.zadd(s,map);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Set zrange(String s, long l, long l1) {
		Jedis jedis=null;
		Set value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.zrange(s,l,l1);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Long zrem(String s, String... as) {
		   if(readOnly)
	            return readOnlyDefaultLongValue;
		Jedis jedis=null;
		Long value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.zrem(s,as);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Double zincrby(String s, double d, String s1) {
		   if(readOnly)
	            return readOnlyDefaultDoubleValue;
		Jedis jedis=null;
		Double value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.zincrby(s,d,s1);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Long zrank(String s, String s1) {
		Jedis jedis=null;
		Long value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.zrank(s,s1);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Long zrevrank(String s, String s1) {
		Jedis jedis=null;
		Long value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.zrevrank(s,s1);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Set zrevrange(String s, long l, long l1) {
		Jedis jedis=null;
		Set value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.zrevrange(s,l,l1);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Set zrangeWithScores(String s, long l, long l1) {
		Jedis jedis=null;
		Set value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.zrangeWithScores(s,l,l1);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Set zrevrangeWithScores(String s, long l, long l1) {
		Jedis jedis=null;
		Set value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.zrevrangeWithScores(s,l,l1);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Long zcard(String s) {
		Jedis jedis=null;
		Long value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.zcard(s);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Double zscore(String s, String s1) {
		Jedis jedis=null;
		Double value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.zscore(s,s1);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public List sort(String s) {
		Jedis jedis=null;
		List value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.sort(s);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Long zcount(String s, double d, double d1) {
		Jedis jedis=null;
		Long value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.zcount(s,d,d1);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Long zcount(String s, String s1, String s2) {
		Jedis jedis=null;
		Long value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.zcount(s,s1,s2);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Set zrangeByScore(String s, double d, double d1) {
		Jedis jedis=null;
		Set value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.zrangeByScore(s,d,d1);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Set zrangeByScore(String s, String s1, String s2) {
		Jedis jedis=null;
		Set value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.zrangeByScore(s,s1,s2);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Set zrevrangeByScore(String s, double d, double d1) {
		Jedis jedis=null;
		Set value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.zrevrangeByScore(s,d,d1);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Set zrangeByScore(String s, double d, double d1, int i, int j) {
		Jedis jedis=null;
		Set value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.zrangeByScore(s,d,d1,i,j);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Set zrevrangeByScore(String s, String s1, String s2) {
		Jedis jedis=null;
		Set value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.zrevrangeByScore(s,s1,s2);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Set zrangeByScore(String s, String s1, String s2, int i, int j) {
		Jedis jedis=null;
		Set value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.zrangeByScore(s,s1,s2,i,j);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Set zrevrangeByScore(String s, double d, double d1, int i, int j) {
		Jedis jedis=null;
		Set value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.zrevrangeByScore(s,d,d1,i,j);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Set zrangeByScoreWithScores(String s, double d, double d1) {
		Jedis jedis=null;
		Set value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.zrangeByScoreWithScores(s,d,d1);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Set zrevrangeByScoreWithScores(String s, double d, double d1) {
		Jedis jedis=null;
		Set value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.zrevrangeByScoreWithScores(s,d,d1);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Set zrangeByScoreWithScores(String s, double d, double d1, int i,
			int j) {
		Jedis jedis=null;
		Set value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.zrangeByScoreWithScores(s,d,d1,i,j);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Set zrevrangeByScore(String s, String s1, String s2, int i, int j) {
		Jedis jedis=null;
		Set value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.zrevrangeByScore(s,s1,s2,i,j);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Set zrangeByScoreWithScores(String s, String s1, String s2) {
		Jedis jedis=null;
		Set value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.zrangeByScoreWithScores(s,s1,s2);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Set zrevrangeByScoreWithScores(String s, String s1, String s2) {
		Jedis jedis=null;
		Set value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.zrevrangeByScoreWithScores(s,s1,s2);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Set zrangeByScoreWithScores(String s, String s1, String s2, int i,
			int j) {
		Jedis jedis=null;
		Set value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.zrangeByScoreWithScores(s,s1,s2,i,j);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Set zrevrangeByScoreWithScores(String s, double d, double d1, int i,
			int j) {
		Jedis jedis=null;
		Set value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.zrevrangeByScoreWithScores(s,d,d1,i,j);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Set zrevrangeByScoreWithScores(String s, String s1, String s2,
			int i, int j) {
		Jedis jedis=null;
		Set value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.zrevrangeByScoreWithScores(s,s1,s2,i,j);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Long zremrangeByRank(String s, long l, long l1) {
		Jedis jedis=null;
		Long value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.zremrangeByRank(s,l,l1);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Long zremrangeByScore(String s, double d, double d1) {
		Jedis jedis=null;
		Long value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.zremrangeByScore(s,d,d1);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Long zremrangeByScore(String s, String s1, String s2) {
		Jedis jedis=null;
		Long value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.zremrangeByScore(s,s1,s2);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Long lpushx(String s, String s1) {
		  if(readOnly)
	            return readOnlyDefaultLongValue;
		Jedis jedis=null;
		Long value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.lpushx(s,s1);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	@Override
	public Long rpushx(String s, String s1) {
		  if(readOnly)
	            return readOnlyDefaultLongValue;
		Jedis jedis=null;
		Long value=null;
		boolean isBroken=false;
		try {
			jedis=(Jedis)conn.getResource();
			value=jedis.rpushx(s,s1);
		} catch (Exception e) {
			isBroken=true;
			throw new JedisException((new StringBuilder()).append(toString()).append(e).toString());
		}finally{
			if(null!=jedis){
				conn.freeResource(jedis, isBroken);
			}
		}
		return value;
	}

	public Jedis getJedis(String key){
		if(null==key){
			key=DEFAULT_SERVER_KEY;
		}
		try {
			String newKey=sanitizeKey(key);
			return (Jedis)getJedis(getClient(newKey));
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public Jedis getJedis(Redis4JClient redis4JClient){
		return (Jedis)redis4JClient.conn.getResource();
	}
	
	public org.apache.commons.pool.impl.GenericObjectPool.Config getPoolConfig() {
		return poolConfig;
	}
	public void setPoolConfig(
			org.apache.commons.pool.impl.GenericObjectPool.Config poolConfig) {
		this.poolConfig = poolConfig;
		  if(server != null){
	            conn = new Redis4JPool(poolConfig, new Redis4JFactory(server));
		  }
	}
	public BasePool getConn() {
		return conn;
	}
	public void setConn(BasePool conn) {
		this.conn = conn;
		if(server == null){
	       server = this.conn.toString();
		}
	}
	public String getServer() {
		return server;
	}
	public void setServer(String server) {
		this.server = server;
	}
	public boolean isThrowJedisException() {
		return throwJedisException;
	}
	public void setThrowJedisException(boolean throwJedisException) {
		this.throwJedisException = throwJedisException;
	}
	public boolean isReadOnly() {
		return readOnly;
	}
	public void setReadOnly(boolean readOnly) {
		this.readOnly = readOnly;
	}
	public long getHashMin() {
		return hashMin;
	}
	public void setHashMin(long hashMin) {
		this.hashMin = hashMin;
	}
	public long getHashMax() {
		return hashMax;
	}
	public void setHashMax(long hashMax) {
		this.hashMax = hashMax;
	}
	public static Long getReadonlydefaultlongvalue() {
		return readOnlyDefaultLongValue;
	}
	public static Boolean getReadonlydefaultbooleanvalue() {
		return readOnlyDefaultBooleanValue;
	}
	@Override
	public Redis4JClient getClient(String key) {
			return this;//the sub class must be overwrite this method
	}
  
}
