package org.jinn.redis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.exceptions.JedisException;

public class Redis4JHAServer extends Redis4JClient {
	
    protected org.apache.commons.pool.impl.GenericObjectPool.Config poolConfig;
    
    protected List servers;
    protected String firstServer;
    protected Redis4JClient first;
    protected String secondServer;
    
    protected Redis4JClient second;
    protected  boolean doubleWrite;
    protected  boolean setSecond;
    
	public org.apache.commons.pool.impl.GenericObjectPool.Config getPoolConfig() {
		return poolConfig;
	}
	public void setPoolConfig(
			org.apache.commons.pool.impl.GenericObjectPool.Config poolConfig) {
		super.setPoolConfig(poolConfig);
		this.poolConfig = poolConfig;
		 init();
	}
	public List getServers() {
		return servers;
	}
	public void setServers(List servers) {
		this.servers = servers;
	}
	public String getFirstServer() {
		return firstServer;
	}
	public void setFirstServer(String firstServer) {
		this.firstServer = firstServer;
	}
	public Redis4JClient getFirst() {
		return first;
	}
	public void setFirst(Redis4JClient first) {
		this.first = first;
	}
	public String getSecondServer() {
		return secondServer;
	}
	public void setSecondServer(String secondServer) {
		this.secondServer = secondServer;
	}
	public Redis4JClient getSecond() {
		return second;
	}
	public void setSecond(Redis4JClient second) {
		this.second = second;
	}
	public boolean isDoubleWrite() {
		return doubleWrite;
	}
	public void setDoubleWrite(boolean doubleWrite) {
		this.doubleWrite = doubleWrite;
	}
	public boolean isSetSecond() {
		return setSecond;
	}
	public void setSetSecond(boolean setSecond) {
		this.setSecond = setSecond;
	}
	public List<Redis4JClient> getClients(){
        List clients = new ArrayList();
        clients.add(first);
        clients.add(second);
        return clients;
	}
	
	public void setReadOnly(boolean readOnly){
	        super.setReadOnly(readOnly);
	        this.readOnly = readOnly;
	        if(first != null)
	            first.setReadOnly(readOnly);
	        if(second != null)
	            second.setReadOnly(readOnly);
	}

	public boolean isAlive(){
	        boolean result = false;
	        if(first != null)
	            result |= first.isAlive();
	        if(second != null)
	            result |= second.isAlive();
	        return result;
	}

	
    private void init(){
        if(servers == null || poolConfig == null)
            return;
        if(servers.size() < 1){
            log.warn("server list empty!");
            return;
        }
        firstServer = (String)servers.get(0);
        first = initConnect(firstServer);
        first.throwJedisException = true;
        first.setReadOnly(readOnly);
        conn = first.conn;
        if(servers.size() > 1){
            secondServer = (String)servers.get(1);
            second = initConnect(secondServer);
            second.setReadOnly(readOnly);
        }
        if(servers.size() > 2){
            log.warn((new StringBuilder()).append("not support for more then 2 servers for now: ").append(servers).toString());
        }
    }

    private Redis4JClient initConnect(String server){
        log.info((new StringBuilder()).append("init connection to ").append(server).toString());
        Redis4JClient p = new Redis4JClient();
        p.setConn(new Redis4JPool(poolConfig, new Redis4JFactory(server)));
        p.setServer(server);
        return p;
    }
    

	@Override
	public String set(String s, String s1) {
		String result=null;
		String result2=null;
	    if(first != null && first.isAlive())
            result = first.set(s, s1);
        if(doubleWrite && second != null && second.isAlive())
            if(setSecond)
            {
                if(result != null)
                    result2 = second.set(s, s1);
            } else
            {
                result2 = second.set(s, s1);
            }
        if(result != null)
            return result;
        if(result2 != null)
            return result2;
        else
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
	}

	@Override
	public String get(String s) {
		// TODO Auto-generated method stub
	    if(first != null && first.isAlive())
            return first.get(s);
	    if(second != null && second.isAlive())
        {
            return second.get(s);
        } else
        {
            log.warn((new StringBuilder()).append(first).append(" [redis server all dead] get, key: ").append(s).toString());
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
        }
	}

	@Override
	public Boolean exists(String s) {
		// TODO Auto-generated method stub
		Boolean result=false;
		Boolean result2=false;
	    if(first != null && first.isAlive())
            result = first.exists(s);
        if(result != null)
            return result;
        if(second != null && second.isAlive()){
        	return second.exists(s);
        }
        else{
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
        }
	}

	@Override
	public String type(String s) {
		// TODO Auto-generated method stub
		String result=null;
		String result2=null;
	    if(first != null && first.isAlive())
            result = first.type(s);
        if(result != null)
            return result;
        if(second != null && second.isAlive()){
        	return second.type(s);
        }
        else{
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
        }
	}

	@Override
	public Long expire(String s, int i) {
		// TODO Auto-generated method stub
		Long result=null;
		Long result2=null;
	    if(first != null && first.isAlive())
            result = first.expire(s,i);
        if(result != null)
            return result;
        if(second != null && second.isAlive()){
        	return second.expire(s,i);
        }
        else{
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
        }
	}

	@Override
	public Long expireAt(String s, long l) {
		// TODO Auto-generated method stub
		Long result=null;
		Long result2=null;
	    if(first != null && first.isAlive())
            result = first.expireAt(s,l);
        if(result != null)
            return result;
        if(second != null && second.isAlive()){
        	return second.expireAt(s,l);
        }
        else{
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
        }
	}

	@Override
	public Long ttl(String s) {
		// TODO Auto-generated method stub
		Long result=null;
		Long result2=null;
	    if(first != null && first.isAlive())
            result = first.ttl(s);
        if(result != null)
            return result;
        if(second != null && second.isAlive()){
        	return second.ttl(s);
        }
        else{
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
        }
	}

	@Override
	public Long setrange(String s, long l, String s1) {
		// TODO Auto-generated method stub
		Long result=null;
		Long result2=null;
	    if(first != null && first.isAlive())
            result = first.setrange(s, l,s1);
        if(doubleWrite && second != null && second.isAlive()){
                if(result != null)
                    result2 = second.setrange(s,l, s1);
        }
        if(result != null)
            return result;
        if(result2 != null)
            return result2;
        else
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
	}

	@Override
	public String getrange(String s, long l, long l1) {
		// TODO Auto-generated method stub
		String result=null;
		String result2=null;
	    if(first != null && first.isAlive())
            result = first.getrange(s,l,l1);
        if(result != null)
            return result;
        if(second != null && second.isAlive()){
        	if(result!=null)
        		result2 = second.getrange(s,l,l1);
        	return result2;
        }
        else{
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
        }
	}

	@Override
	public String getSet(String s, String s1) {
		// TODO Auto-generated method stub
		String result=null;
		String result2=null;
	    if(first != null && first.isAlive())
            result = first.getSet(s,s1);
        if(result != null)
            return result;
        if(second != null && second.isAlive()){
        	if(result!=null)
        		result2 = second.getSet(s,s1);
        	return result2;
        }
        else{
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
        }
	}

	@Override
	public Long setnx(String s, String s1) {
		// TODO Auto-generated method stub
		Long result=null;
		Long result2=null;
	    if(first != null && first.isAlive())
            result = first.setnx(s,s1);
        if(doubleWrite && second != null && second.isAlive()){
                if(result != null)
                    result2 = second.setnx(s,s1);
        }
        if(result != null)
            return result;
        if(result2 != null)
            return result2;
        else
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
	}

	@Override
	public String setex(String s, int i, String s1) {
		// TODO Auto-generated method stub
		String result=null;
		String result2=null;
	    if(first != null && first.isAlive())
            result = first.setex(s,i,s1);
        if(doubleWrite && second != null && second.isAlive()){
                if(result != null)
                    result2 = second.setex(s,i,s1);
        }
        if(result != null)
            return result;
        if(result2 != null)
            return result2;
        else
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
	}

	@Override
	public Long decrBy(String s, long l) {
		// TODO Auto-generated method stub
		Long result=null;
		Long result2=null;
	    if(first != null && first.isAlive())
            result = first.decrBy(s,l);
        if(doubleWrite && second != null && second.isAlive()){
                if(result != null)
                    result2 = second.decrBy(s,l);
        }
        if(result != null)
            return result;
        if(result2 != null)
            return result2;
        else
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
	}

	@Override
	public Long decr(String s) {
		// TODO Auto-generated method stub
		Long result=null;
		Long result2=null;
	    if(first != null && first.isAlive())
            result = first.decr(s);
        if(doubleWrite && second != null && second.isAlive()){
                if(result != null)
                    result2 = second.decr(s);
        }
        if(result != null)
            return result;
        if(result2 != null)
            return result2;
        else
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
	}

	@Override
	public Long incrBy(String s, long l) {
		// TODO Auto-generated method stub
		Long result=null;
		Long result2=null;
	    if(first != null && first.isAlive())
            result = first.incrBy(s,l);
        if(doubleWrite && second != null && second.isAlive()){
                if(result != null)
                    result2 = second.incrBy(s,l);
        }
        if(result != null)
            return result;
        if(result2 != null)
            return result2;
        else
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
	}

	@Override
	public Long incr(String s) {
		// TODO Auto-generated method stub
		Long result=null;
		Long result2=null;
	    if(first != null && first.isAlive())
            result = first.incr(s);
        if(doubleWrite && second != null && second.isAlive()){
                if(result != null)
                    result2 = second.incr(s);
        }
        if(result != null)
            return result;
        if(result2 != null)
            return result2;
        else
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
	}

	@Override
	public Long append(String s, String s1) {
		// TODO Auto-generated method stub
		Long result=null;
		Long result2=null;
	    if(first != null && first.isAlive())
            result = first.append(s,s1);
        if(doubleWrite && second != null && second.isAlive()){
                if(result != null)
                    result2 = second.append(s,s1);
        }
        if(result != null)
            return result;
        if(result2 != null)
            return result2;
        else
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
	}

	@Override
	public String substr(String s, int i, int j) {
		// TODO Auto-generated method stub
		String result=null;
		String result2=null;
	    if(first != null && first.isAlive())
            result = first.substr(s,i,j);
        if(doubleWrite && second != null && second.isAlive()){
                if(result != null)
                    result2 = second.substr(s,i,j);
        }
        if(result != null)
            return result;
        if(result2 != null)
            return result2;
        else
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
	}

	@Override
	public Long hset(String s, String s1, String s2) {
		// TODO Auto-generated method stub
		Long result=null;
		Long result2=null;
	    if(first != null && first.isAlive())
            result = first.hset(s,s1,s2);
        if(doubleWrite && second != null && second.isAlive()){
                if(result != null)
                    result2 = second.hset(s,s1,s2);
        }
        if(result != null)
            return result;
        if(result2 != null)
            return result2;
        else
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
	}

	@Override
	public String hget(String s, String s1) {
		String result=null;
		String result2=null;
	    if(first != null && first.isAlive())
            result = first.hget(s,s1);
        if(result != null)
            return result;
        if(second != null && second.isAlive()){
        	return second.hget(s,s1);
        }
        else{
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
        }
	}

	@Override
	public Long hsetnx(String s, String s1, String s2) {
		Long result=null;
		Long result2=null;
	    if(first != null && first.isAlive())
            result = first.hsetnx(s,s1,s2);
        if(doubleWrite && second != null && second.isAlive()){
                if(result != null)
                    result2 = second.hsetnx(s,s1,s2);
        }
        if(result != null)
            return result;
        if(result2 != null)
            return result2;
        else
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
	}

	@Override
	public String hmset(String s, Map map) {
		String result=null;
		String result2=null;
	    if(first != null && first.isAlive())
            result = first.hmset(s,map);
        if(doubleWrite && second != null && second.isAlive()){
                if(result != null)
                    result2 = second.hmset(s,map);
        }
        if(result != null)
            return result;
        if(result2 != null)
            return result2;
        else
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
	}

	@Override
	public List hmget(String s, String... as) {
		List result=null;
		List result2=null;
	    if(first != null && first.isAlive())
            result = first.hmget(s,as);
        if(result != null)
            return result;
        if(second != null && second.isAlive()){
        	return second.hmget(s,as);
        }
        else{
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
        }
	}

	@Override
	public Long hincrBy(String s, String s1, long l) {
		Long result=null;
		Long result2=null;
	    if(first != null && first.isAlive())
            result = first.hincrBy(s,s1,l);
        if(doubleWrite && second != null && second.isAlive()){
                if(result != null)
                    result2 = second.hincrBy(s,s1,l);
        }
        if(result != null)
            return result;
        if(result2 != null)
            return result2;
        else
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
	}

	@Override
	public Boolean hexists(String s, String s1) {
		Boolean result=null;
		Boolean result2=null;
	    if(first != null && first.isAlive())
            result = first.hexists(s,s1);
        if(result != null)
            return result;
        if(second != null && second.isAlive()){
        	return second.hexists(s,s1);
        }
        else{
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
        }
	}

	@Override
	public Long hdel(String s, String... as) {
		Long result=null;
		Long result2=null;
	    if(first != null && first.isAlive())
            result = first.hdel(s,as);
        if(doubleWrite && second != null && second.isAlive()){
                if(result != null)
                    result2 = second.hdel(s,as);
        }
        if(result != null)
            return result;
        if(result2 != null)
            return result2;
        else
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
	}

	@Override
	public Long hlen(String s) {
		Long result=null;
		Long result2=null;
	    if(first != null && first.isAlive())
            result = first.hlen(s);
        if(result != null)
            return result;
        if(second != null && second.isAlive()){
        	return second.hlen(s);
        }
        else{
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
        }
	}

	@Override
	public Set hkeys(String s) {
		Set result=null;
		Set result2=null;
	    if(first != null && first.isAlive())
            result = first.hkeys(s);
        if(result != null)
            return result;
        if(second != null && second.isAlive()){
        	return second.hkeys(s);
        }
        else{
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
        }
	}

	@Override
	public List hvals(String s) {
		List result=null;
		List result2=null;
	    if(first != null && first.isAlive())
            result = first.hvals(s);
        if(result != null)
            return result;
        if(second != null && second.isAlive()){
        	return second.hvals(s);
        }
        else{
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
        }
	}

	@Override
	public Map hgetAll(String s) {
		Map result=null;
		Map result2=null;
	    if(first != null && first.isAlive())
            result = first.hgetAll(s);
        if(result != null)
            return result;
        if(second != null && second.isAlive()){
        	return second.hgetAll(s);
        }
        else{
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
        }
	}

	@Override
	public Long rpush(String s, String... as) {
		Long result=null;
		Long result2=null;
	    if(first != null && first.isAlive())
            result = first.rpush(s,as);
        if(doubleWrite && second != null && second.isAlive()){
                if(result != null)
                    result2 = second.rpush(s,as);
        }
        if(result != null)
            return result;
        if(result2 != null)
            return result2;
        else
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
	}

	@Override
	public Long lpush(String s, String... as) {
		Long result=null;
		Long result2=null;
	    if(first != null && first.isAlive())
            result = first.lpush(s,as);
        if(doubleWrite && second != null && second.isAlive()){
                if(result != null)
                    result2 = second.lpush(s,as);
        }
        if(result != null)
            return result;
        if(result2 != null)
            return result2;
        else
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
	}

	@Override
	public Long llen(String s) {
		Long result=null;
		Long result2=null;
	    if(first != null && first.isAlive())
            result = first.llen(s);
        if(result != null)
            return result;
        if(second != null && second.isAlive()){
        	return second.llen(s);
        }
        else{
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
        }
	}

	@Override
	public List lrange(String s, long l, long l1) {
		List result=null;
		List result2=null;
	    if(first != null && first.isAlive())
            result = first.lrange(s,l,l1);
        if(result != null)
            return result;
        if(second != null && second.isAlive()){
        	return second.lrange(s,l,l1);
        }
        else{
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
        }
	}

	@Override
	public String ltrim(String s, long l, long l1) {
		String result=null;
		String result2=null;
	    if(first != null && first.isAlive())
            result = first.ltrim(s,l,l1);
        if(result != null)
            return result;
        if(second != null && second.isAlive()){
        	return second.ltrim(s,l,l1);
        }
        else{
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
        }
	}

	@Override
	public String lindex(String s, long l) {
		String result=null;
		String result2=null;
	    if(first != null && first.isAlive())
            result = first.lindex(s,l);
        if(result != null)
            return result;
        if(second != null && second.isAlive()){
        	return second.lindex(s,l);
        }
        else{
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
        }
	}

	@Override
	public String lset(String s, long l, String s1) {
		String result=null;
		String result2=null;
	    if(first != null && first.isAlive())
            result = first.lset(s,l,s1);
        if(doubleWrite && second != null && second.isAlive()){
                if(result != null)
                    result2 = second.lset(s,l,s1);
        }
        if(result != null)
            return result;
        if(result2 != null)
            return result2;
        else
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
	}

	@Override
	public Long lrem(String s, long l, String s1) {
		Long result=null;
		Long result2=null;
	    if(first != null && first.isAlive())
            result = first.lrem(s,l,s1);
        if(doubleWrite && second != null && second.isAlive()){
                if(result != null)
                    result2 = second.lrem(s,l,s1);
        }
        if(result != null)
            return result;
        if(result2 != null)
            return result2;
        else
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
	}

	@Override
	public String lpop(String s) {
		String result=null;
		String result2=null;
	    if(first != null && first.isAlive())
            result = first.lpop(s);
        if(doubleWrite && second != null && second.isAlive()){
                if(result != null)
                    result2 = second.lpop(s);
        }
        if(result != null)
            return result;
        if(result2 != null)
            return result2;
        else
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
	}

	@Override
	public String rpop(String s) {
		String result=null;
		String result2=null;
	    if(first != null && first.isAlive())
            result = first.rpop(s);
        if(doubleWrite && second != null && second.isAlive()){
                if(result != null)
                    result2 = second.rpop(s);
        }
        if(result != null)
            return result;
        if(result2 != null)
            return result2;
        else
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
	}

	@Override
	public Long sadd(String s, String... as) {
		Long result=null;
		Long result2=null;
	    if(first != null && first.isAlive())
            result = first.sadd(s,as);
        if(doubleWrite && second != null && second.isAlive()){
                if(result != null)
                    result2 = second.sadd(s,as);
        }
        if(result != null)
            return result;
        if(result2 != null)
            return result2;
        else
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
	}

	@Override
	public Set smembers(String s) {
		Set result=null;
		Set result2=null;
	    if(first != null && first.isAlive())
            result = first.smembers(s);
        if(result != null)
            return result;
        if(second != null && second.isAlive()){
        	return second.smembers(s);
        }
        else{
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
        }
	}

	@Override
	public Long srem(String s, String... as) {
		Long result=null;
		Long result2=null;
	    if(first != null && first.isAlive())
            result = first.srem(s,as);
        if(doubleWrite && second != null && second.isAlive()){
                if(result != null)
                    result2 = second.srem(s,as);
        }
        if(result != null)
            return result;
        if(result2 != null)
            return result2;
        else
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
	}

	@Override
	public String spop(String s) {
		String result=null;
		String result2=null;
	    if(first != null && first.isAlive())
            result = first.spop(s);
        if(doubleWrite && second != null && second.isAlive()){
                if(result != null)
                    result2 = second.spop(s);
        }
        if(result != null)
            return result;
        if(result2 != null)
            return result2;
        else
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
	}

	@Override
	public Long scard(String s) {
		Long result=null;
		Long result2=null;
	    if(first != null && first.isAlive())
            result = first.scard(s);
        if(result != null)
            return result;
        if(second != null && second.isAlive()){
        	return second.scard(s);
        }
        else{
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
        }
	}

	@Override
	public Boolean sismember(String s, String s1) {
		Boolean result=null;
		Boolean result2=null;
	    if(first != null && first.isAlive())
            result = first.sismember(s,s1);
        if(result != null)
            return result;
        if(second != null && second.isAlive()){
        	return second.sismember(s,s1);
        }
        else{
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
        }
	}

	@Override
	public String srandmember(String s) {
		String result=null;
		String result2=null;
	    if(first != null && first.isAlive())
            result = first.srandmember(s);
        if(result != null)
            return result;
        if(second != null && second.isAlive()){
        	return second.srandmember(s);
        }
        else{
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
        }
	}

	@Override
	public Long zadd(String s, double d, String s1) {
		Long result=null;
		Long result2=null;
	    if(first != null && first.isAlive())
            result = first.zadd(s,d,s1);
        if(doubleWrite && second != null && second.isAlive()){
                if(result != null)
                    result2 = second.zadd(s,d,s1);
        }
        if(result != null)
            return result;
        if(result2 != null)
            return result2;
        else
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
	}

	@Override
	public Long zadd(String s, Map map) {
		Long result=null;
		Long result2=null;
	    if(first != null && first.isAlive())
            result = first.zadd(s,map);
        if(doubleWrite && second != null && second.isAlive()){
                if(result != null)
                    result2 = second.zadd(s,map);
        }
        if(result != null)
            return result;
        if(result2 != null)
            return result2;
        else
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
	}

	@Override
	public Set zrange(String s, long l, long l1) {
		Set result=null;
		Set result2=null;
	    if(first != null && first.isAlive())
            result = first.zrange(s,l,l1);
        if(result != null)
            return result;
        if(second != null && second.isAlive()){
        	return second.zrange(s,l,l1);
        }
        else{
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
        }
	}

	@Override
	public Long zrem(String s, String... as) {
		Long result=null;
		Long result2=null;
	    if(first != null && first.isAlive())
            result = first.zrem(s,as);
        if(doubleWrite && second != null && second.isAlive()){
                if(result != null)
                    result2 = second.zrem(s,as);
        }
        if(result != null)
            return result;
        if(result2 != null)
            return result2;
        else
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
	}

	@Override
	public Double zincrby(String s, double d, String s1) {
		Double result=null;
		Double result2=null;
	    if(first != null && first.isAlive())
            result = first.zincrby(s,d,s1);
        if(doubleWrite && second != null && second.isAlive()){
                if(result != null)
                    result2 = second.zincrby(s,d,s1);
        }
        if(result != null)
            return result;
        if(result2 != null)
            return result2;
        else
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
	}

	@Override
	public Long zrank(String s, String s1) {
		Long result=null;
		Long result2=null;
	    if(first != null && first.isAlive())
            result = first.zrank(s,s1);
        if(result != null)
            return result;
        if(second != null && second.isAlive()){
        	return second.zrank(s,s1);
        }
        else{
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
        }
	}

	@Override
	public Long zrevrank(String s, String s1) {
		Long result=null;
		Long result2=null;
	    if(first != null && first.isAlive())
            result = first.zrevrank(s,s1);
        if(result != null)
            return result;
        if(second != null && second.isAlive()){
        	return second.zrevrank(s,s1);
        }
        else{
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
        }
	}

	@Override
	public Set zrevrange(String s, long l, long l1) {
		Set result=null;
		Set result2=null;
	    if(first != null && first.isAlive())
            result = first.zrevrange(s,l,l1);
        if(result != null)
            return result;
        if(second != null && second.isAlive()){
        	return second.zrevrange(s,l,l1);
        }
        else{
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
        }
	}

	@Override
	public Set zrangeWithScores(String s, long l, long l1) {
		Set result=null;
		Set result2=null;
	    if(first != null && first.isAlive())
            result = first.zrangeWithScores(s,l,l1);
        if(result != null)
            return result;
        if(second != null && second.isAlive()){
        	return second.zrangeWithScores(s,l,l1);
        }
        else{
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
        }
	}

	@Override
	public Set zrevrangeWithScores(String s, long l, long l1) {
		Set result=null;
		Set result2=null;
	    if(first != null && first.isAlive())
            result = first.zrevrangeWithScores(s,l,l1);
        if(result != null)
            return result;
        if(second != null && second.isAlive()){
        	return second.zrevrangeWithScores(s,l,l1);
        }
        else{
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
        }
	}

	@Override
	public Long zcard(String s) {
		Long result=null;
		Long result2=null;
	    if(first != null && first.isAlive())
            result = first.zcard(s);
        if(result != null)
            return result;
        if(second != null && second.isAlive()){
        	return second.zcard(s);
        }
        else{
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
        }
	}

	@Override
	public Double zscore(String s, String s1) {
		Double result=null;
		Double result2=null;
	    if(first != null && first.isAlive())
            result = first.zscore(s,s1);
        if(result != null)
            return result;
        if(second != null && second.isAlive()){
        	return second.zscore(s,s1);
        }
        else{
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
        }
	}

	@Override
	public List sort(String s) {
		List result=null;
		List result2=null;
	    if(first != null && first.isAlive())
            result = first.sort(s);
        if(result != null)
            return result;
        if(second != null && second.isAlive()){
        	return second.sort(s);
        }
        else{
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
        }
	}

	@Override
	public Long zcount(String s, double d, double d1) {
		Long result=null;
		Long result2=null;
	    if(first != null && first.isAlive())
            result = first.zcount(s,d,d1);
        if(result != null)
            return result;
        if(second != null && second.isAlive()){
        	return second.zcount(s,d,d1);
        }
        else{
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
        }
	}

	@Override
	public Long zcount(String s, String s1, String s2) {
		Long result=null;
		Long result2=null;
	    if(first != null && first.isAlive())
            result = first.zcount(s,s1,s2);
        if(result != null)
            return result;
        if(second != null && second.isAlive()){
        	return second.zcount(s,s1,s2);
        }
        else{
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
        }
	}

	@Override
	public Set zrangeByScore(String s, double d, double d1) {
		Set result=null;
		Set result2=null;
	    if(first != null && first.isAlive())
            result = first.zrangeByScore(s,d,d1);
        if(result != null)
            return result;
        if(second != null && second.isAlive()){
        	return second.zrangeByScore(s,d,d1);
        }
        else{
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
        }
	}

	@Override
	public Set zrangeByScore(String s, String s1, String s2) {
		Set result=null;
		Set result2=null;
	    if(first != null && first.isAlive())
            result = first.zrangeByScore(s,s1,s2);
        if(result != null)
            return result;
        if(second != null && second.isAlive()){
        	return second.zrangeByScore(s,s1,s2);
        }
        else{
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
        }
	}

	@Override
	public Set zrevrangeByScore(String s, double d, double d1) {
		Set result=null;
		Set result2=null;
	    if(first != null && first.isAlive())
            result = first.zrevrangeByScore(s,d,d1);
        if(result != null)
            return result;
        if(second != null && second.isAlive()){
        	return second.zrevrangeByScore(s,d,d1);
        }
        else{
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
        }
	}

	@Override
	public Set zrangeByScore(String s, double d, double d1, int i, int j) {
		Set result=null;
		Set result2=null;
	    if(first != null && first.isAlive())
            result = first.zrangeByScore(s,d,d1,i,j);
        if(result != null)
            return result;
        if(second != null && second.isAlive()){
        	return second.zrangeByScore(s,d,d1,i,j);
        }
        else{
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
        }
	}

	@Override
	public Set zrevrangeByScore(String s, String s1, String s2) {
		Set result=null;
		Set result2=null;
	    if(first != null && first.isAlive())
            result = first.zrevrangeByScore(s,s1,s2);
        if(result != null)
            return result;
        if(second != null && second.isAlive()){
        	return second.zrevrangeByScore(s,s1,s2);
        }
        else{
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
        }
	}

	@Override
	public Set zrangeByScore(String s, String s1, String s2, int i, int j) {
		Set result=null;
		Set result2=null;
	    if(first != null && first.isAlive())
            result = first.zrangeByScore(s,s1,s2,i,j);
        if(result != null)
            return result;
        if(second != null && second.isAlive()){
        	return second.zrangeByScore(s,s1,s2,i,j);
        }
        else{
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
        }
	}

	@Override
	public Set zrevrangeByScore(String s, double d, double d1, int i, int j) {
		Set result=null;
		Set result2=null;
	    if(first != null && first.isAlive())
            result = first.zrevrangeByScore(s,d,d1,i,j);
        if(result != null)
            return result;
        if(second != null && second.isAlive()){
        	return second.zrevrangeByScore(s,d,d1,i,j);
        }
        else{
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
        }
	}

	@Override
	public Set zrangeByScoreWithScores(String s, double d, double d1) {
		Set result=null;
		Set result2=null;
	    if(first != null && first.isAlive())
            result = first.zrangeByScoreWithScores(s,d,d1);
        if(result != null)
            return result;
        if(second != null && second.isAlive()){
        	return second.zrangeByScoreWithScores(s,d,d1);
        }
        else{
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
        }
	}

	@Override
	public Set zrevrangeByScoreWithScores(String s, double d, double d1) {
		Set result=null;
		Set result2=null;
	    if(first != null && first.isAlive())
            result = first.zrevrangeByScoreWithScores(s,d,d1);
        if(result != null)
            return result;
        if(second != null && second.isAlive()){
        	return second.zrevrangeByScoreWithScores(s,d,d1);
        }
        else{
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
        }
	}

	@Override
	public Set zrangeByScoreWithScores(String s, double d, double d1, int i,
			int j) {
		Set result=null;
		Set result2=null;
	    if(first != null && first.isAlive())
            result = first.zrangeByScoreWithScores(s,d,d1,i,j);
        if(result != null)
            return result;
        if(second != null && second.isAlive()){
        	return second.zrangeByScoreWithScores(s,d,d1,i,j);
        }
        else{
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
        }
	}

	@Override
	public Set zrevrangeByScore(String s, String s1, String s2, int i, int j) {
		Set result=null;
		Set result2=null;
	    if(first != null && first.isAlive())
            result = first.zrevrangeByScore(s,s1,s2,i,j);
        if(result != null)
            return result;
        if(second != null && second.isAlive()){
        	return second.zrevrangeByScore(s,s1,s2,i,j);
        }
        else{
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
        }
	}

	@Override
	public Set zrangeByScoreWithScores(String s, String s1, String s2) {
		Set result=null;
		Set result2=null;
	    if(first != null && first.isAlive())
            result = first.zrangeByScoreWithScores(s,s1,s2);
        if(result != null)
            return result;
        if(second != null && second.isAlive()){
        	return second.zrangeByScoreWithScores(s,s1,s2);
        }
        else{
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
        }
	}

	@Override
	public Set zrevrangeByScoreWithScores(String s, String s1, String s2) {
		Set result=null;
		Set result2=null;
	    if(first != null && first.isAlive())
            result = first.zrevrangeByScoreWithScores(s,s1,s2);
        if(result != null)
            return result;
        if(second != null && second.isAlive()){
        	return second.zrevrangeByScoreWithScores(s,s1,s2);
        }
        else{
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
        }
	}

	@Override
	public Set zrangeByScoreWithScores(String s, String s1, String s2, int i,
			int j) {
		Set result=null;
		Set result2=null;
	    if(first != null && first.isAlive())
            result = first.zrangeByScoreWithScores(s,s1,s2,i,j);
        if(result != null)
            return result;
        if(second != null && second.isAlive()){
        	return second.zrangeByScoreWithScores(s,s1,s2,i,j);
        }
        else{
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
        }
	}

	@Override
	public Set zrevrangeByScoreWithScores(String s, double d, double d1, int i,
			int j) {
		Set result=null;
		Set result2=null;
	    if(first != null && first.isAlive())
            result = first.zrevrangeByScoreWithScores(s,d,d1,i,j);
        if(result != null)
            return result;
        if(second != null && second.isAlive()){
        	return second.zrevrangeByScoreWithScores(s,d,d1,i,j);
        }
        else{
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
        }
	}

	@Override
	public Set zrevrangeByScoreWithScores(String s, String s1, String s2,
			int i, int j) {
		Set result=null;
		Set result2=null;
	    if(first != null && first.isAlive())
            result = first.zrevrangeByScoreWithScores(s,s1,s2,i,j);
        if(result != null)
            return result;
        if(second != null && second.isAlive()){
        	return second.zrevrangeByScoreWithScores(s,s1,s2,i,j);
        }
        else{
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
        }
	}

	@Override
	public Long zremrangeByRank(String s, long l, long l1) {
		Long result=null;
		Long result2=null;
	    if(first != null && first.isAlive())
            result = first.zremrangeByRank(s,l,l1);
        if(result != null)
            return result;
        if(second != null && second.isAlive()){
        	return second.zremrangeByRank(s,l,l1);
        }
        else{
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
        }
	}

	@Override
	public Long zremrangeByScore(String s, double d, double d1) {
		Long result=null;
		Long result2=null;
	    if(first != null && first.isAlive())
            result = first.zremrangeByScore(s,d,d1);
        if(result != null)
            return result;
        if(second != null && second.isAlive()){
        	return second.zremrangeByScore(s,d,d1);
        }
        else{
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
        }
	}

	@Override
	public Long zremrangeByScore(String s, String s1, String s2) {
		Long result=null;
		Long result2=null;
	    if(first != null && first.isAlive())
            result = first.zremrangeByScore(s,s1,s2);
        if(result != null)
            return result;
        if(second != null && second.isAlive()){
        	return second.zremrangeByScore(s,s1,s2);
        }
        else{
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
        }
	}

	@Override
	public Long lpushx(String s, String s1) {
		Long result=null;
		Long result2=null;
	    if(first != null && first.isAlive())
            result = first.lpushx(s,s1);
        if(doubleWrite && second != null && second.isAlive()){
                if(result != null)
                    result2 = second.lpushx(s,s1);
        }
        if(result != null)
            return result;
        if(result2 != null)
            return result2;
        else
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
	}

	@Override
	public Long rpushx(String s, String s1) {
		Long result=null;
		Long result2=null;
	    if(first != null && first.isAlive())
            result = first.rpushx(s,s1);
        if(doubleWrite && second != null && second.isAlive()){
                if(result != null)
                    result2 = second.rpushx(s,s1);
        }
        if(result != null)
            return result;
        if(result2 != null)
            return result2;
        else
            throw new JedisException((new StringBuilder()).append("redis server all dead: ").append(first).append(" ").append(second).toString());
	}
}
