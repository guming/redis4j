package org.jinn.redis;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;


public class Redis4JMSServer extends Redis4JClient {
	
	    protected org.apache.commons.pool.impl.GenericObjectPool.Config poolConfig;
	    protected String masterServer;
	    protected Redis4JClient master;
	    protected String slaveServer;
	    protected Redis4JClient slave;
		public org.apache.commons.pool.impl.GenericObjectPool.Config getPoolConfig() {
			return poolConfig;
		}
		public void setPoolConfig(
				org.apache.commons.pool.impl.GenericObjectPool.Config poolConfig) {
			this.poolConfig = poolConfig;
		}
		public String getMasterServer() {
			return masterServer;
		}
		public void setMasterServer(String masterServer) {
			this.masterServer = masterServer;
		}
		public Redis4JClient getMaster() {
			return master;
		}
		public void setMaster(Redis4JClient master) {
			this.master = master;
		}
		public String getSlaveServer() {
			return slaveServer;
		}
		public void setSlaveServer(String slaveServer) {
			this.slaveServer = slaveServer;
		}
		public Redis4JClient getSlave() {
			return slave;
		}
		public void setSlave(Redis4JClient slave) {
			this.slave = slave;
		}
	    
	    public boolean isReadOnly(){
	        return readOnly;
	    }

	    public void setReadOnly(boolean readOnly){
	        super.setReadOnly(readOnly);
	        this.readOnly = readOnly;
	        if(master != null)
	            master.setReadOnly(readOnly);//master可当slave
	        if(slave != null)
	            slave.setReadOnly(true);
	    }

	    public Redis4JClient getClient(boolean isWrite){
	    	if(isWrite){
	    		if(master!=null&&master.isAlive())
	    			return master;
	    		else
	    			return null;
	    	}else{
	    		if(slave!=null&&slave.isAlive())
	    			return slave;
	    		else
	    			return master;
	    	}
	    }
	    
	    public void init(){
	        if(poolConfig == null || StringUtils.isBlank(masterServer))
	            throw new RuntimeException("init jedisMSServer error");
	        throwJedisException = true;
	        master = initConnect(masterServer);
	        master.throwJedisException = true;
	        master.setReadOnly(readOnly);
	        conn = master.conn;
	        if(!StringUtils.isBlank(slaveServer))
	        {
	            slave = initConnect(slaveServer);
	            slave.setReadOnly(true);
	        }
	    }

	    private Redis4JClient initConnect(String server)
	    {
	        log.info((new StringBuilder()).append("init connection to ").append(server).toString());
	        Redis4JClient p = new Redis4JClient();
	        p.setConn(new Redis4JPool(poolConfig, new Redis4JFactory(server)));
	        p.setServer(server);
	        return p;
	    }
	    
	    public boolean isAlive(){
	        boolean result = false;
	        if(master != null)
	            result |= master.isAlive();
	        if(slave != null)
	            result |= slave.isAlive();
	        return result;
	    }
	    
		@Override
		public String set(String s, String s1) {
			return getClient(true).set(s,s1);
		}

		@Override
		public String get(String s) {
			return getClient(false).get(s);
		}

		@Override
		public Boolean exists(String s) {
			return getClient(false).exists(s);
		}

		@Override
		public String type(String s) {
			return getClient(false).type(s);
		}

		@Override
		public Long expire(String s, int i) {
			return getClient(false).expire(s,i);
		}

		@Override
		public Long expireAt(String s, long l) {
			return getClient(false).expireAt(s,l);
		}

		@Override
		public Long ttl(String s) {
			return getClient(false).ttl(s);
		}

		@Override
		public Long setrange(String s, long l, String s1) {
			return getClient(true).setrange(s,l,s1);
		}

		@Override
		public String getrange(String s, long l, long l1) {
			return getClient(false).getrange(s,l,l1);
		}

		@Override
		public String getSet(String s, String s1) {
				return getClient(false).getSet(s,s1);
		}

		@Override
		public Long setnx(String s, String s1) {
				return getClient(true).setnx(s,s1);
		}

		@Override
		public String setex(String s, int i, String s1) {
				return getClient(true).setex(s,i,s1);
		}

		@Override
		public Long decrBy(String s, long l) {
				return getClient(true).decrBy(s,l);
		}

		@Override
		public Long decr(String s) {
				return getClient(true).decr(s);
		}

		@Override
		public Long incrBy(String s, long l) {
				return getClient(true).incrBy(s,l);
		}

		@Override
		public Long incr(String s) {
			return getClient(true).incr(s);
		}

		@Override
		public Long append(String s, String s1) {
			return getClient(true).append(s,s1);
		}

		@Override
		public String substr(String s, int i, int j) {
			return getClient(true).substr(s,i,j);
		}

		@Override
		public Long hset(String s, String s1, String s2) {
			return getClient(true).hset(s,s1,s2);
		}

		@Override
		public String hget(String s, String s1) {
			return getClient(false).hget(s,s1);
		}

		@Override
		public Long hsetnx(String s, String s1, String s2) {
			return getClient(true).hsetnx(s,s1,s2);
		}
		
		@Override
		public String hmset(String s, Map map) {
			return getClient(true).hmset(s,map);
		}

		@Override
		public List hmget(String s, String... as) {
			return getClient(false).hmget(s,as);
		}

		@Override
		public Long hincrBy(String s, String s1, long l) {
			return getClient(true).hincrBy(s,s1,l);
		}

		@Override
		public Boolean hexists(String s, String s1) {
			return getClient(false).hexists(s,s1);
		}

		@Override
		public Long hdel(String s, String... as) {
			return getClient(true).hdel(s,as);
		}

		@Override
		public Long hlen(String s) {
			return getClient(false).hlen(s);
		}

		@Override
		public Set hkeys(String s) {
			return getClient(false).hkeys(s);
		}

		@Override
		public List hvals(String s) {
			return getClient(false).hvals(s);
		}

		@Override
		public Map hgetAll(String s) {
			return getClient(false).hgetAll(s);
		}

		@Override
		public Long rpush(String s, String... as) {
			return getClient(true).rpush(s,as);
		}

		@Override
		public Long lpush(String s, String... as) {
			return getClient(true).lpush(s,as);
		}

		@Override
		public Long llen(String s) {
			return getClient(false).llen(s);
		}

		@Override
		public List lrange(String s, long l, long l1) {
			return getClient(false).lrange(s,l,l1);
		}

		@Override
		public String ltrim(String s, long l, long l1) {
			return getClient(false).ltrim(s,l,l1);
		}

		@Override
		public String lindex(String s, long l) {
			return getClient(false).lindex(s,l);
		}

		@Override
		public String lset(String s, long l, String s1) {
			return getClient(true).lset(s,l,s1);
		}

		@Override
		public Long lrem(String s, long l, String s1) {
			return getClient(true).lrem(s,l,s1);
		}

		@Override
		public String lpop(String s) {
			return getClient(true).lpop(s);
		}

		@Override
		public String rpop(String s) {
			return getClient(true).rpop(s);	
		}

		@Override
		public Long sadd(String s, String... as) {
			return getClient(true).sadd(s,as);
		}

		@Override
		public Set smembers(String s) {
			return getClient(true).smembers(s);
		}

		@Override
		public Long srem(String s, String... as) {
			return getClient(true).srem(s,as);
		}

		@Override
		public String spop(String s) {
			return getClient(true).spop(s);
		}

		@Override
		public Long scard(String s) {
			return getClient(false).scard(s);
		}

		@Override
		public Boolean sismember(String s, String s1) {
			return getClient(false).sismember(s,s1);
		}
		
		@Override
		public String srandmember(String s) {
			return getClient(false).srandmember(s);
		}

		@Override
		public Long zadd(String s, double d, String s1) {
			return getClient(true).zadd(s,d,s1);
		}

		@Override
		public Long zadd(String s, Map map) {
			return getClient(true).zadd(s,map);
		}

		@Override
		public Set zrange(String s, long l, long l1) {
			return getClient(false).zrange(s,l,l1);
		}

		@Override
		public Long zrem(String s, String... as) {
			return getClient(false).zrem(s,as);
		}

		@Override
		public Double zincrby(String s, double d, String s1) {
			return getClient(false).zincrby(s,d,s1);
		}

		@Override
		public Long zrank(String s, String s1) {
			return getClient(false).zrank(s,s1);
		}

		@Override
		public Long zrevrank(String s, String s1) {
			return getClient(false).zrevrank(s,s1);
		}

		@Override
		public Set zrevrange(String s, long l, long l1) {
			return getClient(false).zrevrange(s,l,l1);
		}

		@Override
		public Set zrangeWithScores(String s, long l, long l1) {
			return getClient(false).zrangeWithScores(s,l,l1);
		}

		@Override
		public Set zrevrangeWithScores(String s, long l, long l1) {
			return getClient(false).zrevrangeWithScores(s,l,l1);
		}

		@Override
		public Long zcard(String s) {
			return getClient(false).zcard(s);
		}

		@Override
		public Double zscore(String s, String s1) {
			return getClient(false).zscore(s,s1);
		}

		@Override
		public List sort(String s) {
			return getClient(false).sort(s);
		}

		@Override
		public Long zcount(String s, double d, double d1) {
			return getClient(false).zcount(s,d,d1);
		}

		@Override
		public Long zcount(String s, String s1, String s2) {
				return getClient(false).zcount(s,s1,s2);
		}

		@Override
		public Set zrangeByScore(String s, double d, double d1) {
				return getClient(false).zrangeByScore(s,d,d1);
		}

		@Override
		public Set zrangeByScore(String s, String s1, String s2) {
				return getClient(false).zrangeByScore(s,s1,s2);
		}

		@Override
		public Set zrevrangeByScore(String s, double d, double d1) {
				return getClient(false).zrevrangeByScore(s,d,d1);
		}

		@Override
		public Set zrangeByScore(String s, double d, double d1, int i, int j) {
			return getClient(false).zrangeByScore(s,d,d1,i,j);
		}

		@Override
		public Set zrevrangeByScore(String s, String s1, String s2) {
				return getClient(false).zrevrangeByScore(s,s1,s2);
		}

		@Override
		public Set zrangeByScore(String s, String s1, String s2, int i, int j) {
				return getClient(false).zrangeByScore(s,s1,s2,i,j);
		}

		@Override
		public Set zrevrangeByScore(String s, double d, double d1, int i, int j) {
				return getClient(false).zrevrangeByScore(s,d,d1,i,j);
		}

		@Override
		public Set zrangeByScoreWithScores(String s, double d, double d1) {
				return getClient(false).zrangeByScoreWithScores(s,d,d1);
		}

		@Override
		public Set zrevrangeByScoreWithScores(String s, double d, double d1) {
				return getClient(false).zrevrangeByScoreWithScores(s,d,d1);
		}

		@Override
		public Set zrangeByScoreWithScores(String s, double d, double d1, int i,
				int j) {
				return getClient(false).zrangeByScoreWithScores(s,d,d1,i,j);
		}
		@Override
		public Set zrevrangeByScore(String s, String s1, String s2, int i, int j) {
				return getClient(false).zrevrangeByScore(s,s1,s2,i,j);
		}

		@Override
		public Set zrangeByScoreWithScores(String s, String s1, String s2) {
				return getClient(false).zrangeByScoreWithScores(s,s1,s2);
		}

		@Override
		public Set zrevrangeByScoreWithScores(String s, String s1, String s2) {
				return getClient(false).zrevrangeByScoreWithScores(s,s1,s2);
		}

		@Override
		public Set zrangeByScoreWithScores(String s, String s1, String s2, int i,
				int j) {
				return getClient(false).zrangeByScoreWithScores(s,s1,s2,i,j);
		}

		@Override
		public Set zrevrangeByScoreWithScores(String s, double d, double d1, int i,
				int j) {
			return getClient(false).zrevrangeByScoreWithScores(s,d,d1,i,j);
		}

		@Override
		public Set zrevrangeByScoreWithScores(String s, String s1, String s2,
				int i, int j) {
			return getClient(false).zrevrangeByScoreWithScores(s,s1,s2,i,j);
		}

		@Override
		public Long zremrangeByRank(String s, long l, long l1) {
			return getClient(false).zremrangeByRank(s,l,l1);
		}

		@Override
		public Long zremrangeByScore(String s, double d, double d1) {
				return getClient(false).zremrangeByScore(s,d,d1);
		}

		@Override
		public Long zremrangeByScore(String s, String s1, String s2) {
				return getClient(false).zremrangeByScore(s,s1,s2);
		}

		@Override
		public Long lpushx(String s, String s1) {
			return getClient(true).lpushx(s,s1);
		}

		@Override
		public Long rpushx(String s, String s1) {
			return getClient(true).rpushx(s,s1);
		}
}
