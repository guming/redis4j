package org.jinn.redis;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.jinn.redis.util.ConsistenceHash;

public class Redis4JShardingServer extends Redis4JClient {
	protected org.apache.commons.pool.impl.GenericObjectPool.Config poolConfig;

	protected List<String> servers;
	protected HashMap<String, Redis4JClient> clientMap = new HashMap<String, Redis4JClient>();

	private TreeMap<Long, String> consistentBuckets;
	ConsistenceHash consistence = new ConsistenceHash();

	public org.apache.commons.pool.impl.GenericObjectPool.Config getPoolConfig() {
		return poolConfig;
	}

	public void setPoolConfig(
			org.apache.commons.pool.impl.GenericObjectPool.Config poolConfig) {
		super.setPoolConfig(poolConfig);
		this.poolConfig = poolConfig;
	}

	public List<String> getServers() {
		return servers;
	}

	public void setServers(List<String> servers) {
		this.servers = servers;
	}

	public void start() {
		if (servers == null || poolConfig == null)
			return;
		if (servers.size() < 1) {
			log.warn("server list empty!");
			return;
		}
		for (String server : servers) {
			Redis4JClient sClient = initConnect(server);
			System.out.println(sClient.isAlive());
			if (sClient != null && sClient.isAlive())
				clientMap.put(server, sClient);
		}
		consistentBuckets = consistence.buildMap(servers);
	}

	private Redis4JClient initConnect(String server) {
		log.info((new StringBuilder()).append("init connection to ")
				.append(server).toString());
		Redis4JClient p = new Redis4JClient();
		Redis4JPool rjp = new Redis4JPool(poolConfig,
				new Redis4JFactory(server));
		// rjp.setCheckStatus(true);
		rjp.initPool();
		p.setConn(rjp);
		p.setServer(server);

		return p;
	}

	@Override
	public Redis4JClient getClient(String key) {
		Redis4JClient client = clientMap.get(getServer(key));
		if (client == null || !client.isAlive()) {
			int rehashTries = 0;
			Redis4JClient simpleClient = null;
			while (simpleClient == null || !simpleClient.isAlive()) {
				String newKey = String.format("%s%s", rehashTries, key);
				// if ( log.isDebugEnabled() )
				log.debug("rehashing with: " + newKey);
				simpleClient = clientMap.get(getServer(newKey));
				rehashTries++;
			}
			return simpleClient;
		} else {
			return client;
		}
	}

	public String getServer(String key) {
		final Long hash = consistence.alg.hash(key);
		SortedMap<Long, String> tail = consistentBuckets.tailMap(hash);
		if (tail.size() == 0)
			return consistentBuckets.get(consistentBuckets.firstKey());
		else
			return (String) tail.get(tail.firstKey());
	}

	@Override
	public String set(String s, String s1) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.set(s, s1);

	}

	@Override
	public String get(String s) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.get(s);

	}

	@Override
	public Boolean exists(String s) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.exists(s);

	}

	@Override
	public String type(String s) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.type(s);

	}

	@Override
	public Long expire(String s, int i) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.expire(s, i);

	}

	@Override
	public Long expireAt(String s, long l) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.expireAt(s, l);

	}

	@Override
	public Long ttl(String s) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.ttl(s);

	}

	@Override
	public Long setrange(String s, long l, String s1) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.setrange(s, l, s1);

	}

	@Override
	public String getrange(String s, long l, long l1) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.getrange(s, l, l1);

	}

	@Override
	public String getSet(String s, String s1) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.getSet(s, s1);

	}

	@Override
	public Long setnx(String s, String s1) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.setnx(s, s1);

	}

	@Override
	public String setex(String s, int i, String s1) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.setex(s, i, s1);

	}

	@Override
	public Long decrBy(String s, long l) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.decrBy(s, l);

	}

	@Override
	public Long decr(String s) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.decr(s);

	}

	@Override
	public Long incrBy(String s, long l) {
		// TODO Auto-generated method stub

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.incrBy(s, l);

	}

	@Override
	public Long incr(String s) {
		// TODO Auto-generated method stub

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.incr(s);

	}

	@Override
	public Long append(String s, String s1) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.append(s, s1);

	}

	@Override
	public String substr(String s, int i, int j) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.substr(s, i, j);

	}

	@Override
	public Long hset(String s, String s1, String s2) {
		// TODO Auto-generated method stub

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.hset(s, s1, s2);

	}

	@Override
	public String hget(String s, String s1) {
		// TODO Auto-generated method stub
		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.hget(s, s1);

	}

	@Override
	public Long hsetnx(String s, String s1, String s2) {
		// TODO Auto-generated method stub

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.hsetnx(s, s1, s2);

	}

	@Override
	public String hmset(String s, Map map) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.hmset(s, map);
	}

	@Override
	public List hmget(String s, String... as) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.hmget(s, as);
	}

	@Override
	public Long hincrBy(String s, String s1, long l) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.hincrBy(s, s1, l);

	}

	@Override
	public Boolean hexists(String s, String s1) {
		// TODO Auto-generated method stub

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.hexists(s, s1);

	}

	@Override
	public Long hdel(String s, String... as) {
		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.hdel(s, as);

	}

	@Override
	public Long hlen(String s) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.hlen(s);

	}

	@Override
	public Set hkeys(String s) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.hkeys(s);

	}

	@Override
	public List hvals(String s) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.hvals(s);

	}

	@Override
	public Map hgetAll(String s) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.hgetAll(s);

	}

	@Override
	public Long rpush(String s, String... as) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.rpush(s, as);

	}

	@Override
	public Long lpush(String s, String... as) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.lpush(s, as);

	}

	@Override
	public Long llen(String s) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.llen(s);

	}

	@Override
	public List lrange(String s, long l, long l1) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.lrange(s, l, l1);

	}

	@Override
	public String ltrim(String s, long l, long l1) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.ltrim(s, l, l1);

	}

	@Override
	public String lindex(String s, long l) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.lindex(s, l);

	}

	@Override
	public String lset(String s, long l, String s1) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.lset(s, l, s1);

	}

	@Override
	public Long lrem(String s, long l, String s1) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.lrem(s, l, s1);

	}

	@Override
	public String lpop(String s) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.lpop(s);

	}

	@Override
	public String rpop(String s) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.rpop(s);

	}

	@Override
	public Long sadd(String s, String... as) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.sadd(s, as);

	}

	@Override
	public Set smembers(String s) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.smembers(s);

	}

	@Override
	public Long srem(String s, String... as) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.srem(s, as);

	}

	@Override
	public String spop(String s) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.spop(s);

	}

	@Override
	public Long scard(String s) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.scard(s);

	}

	@Override
	public Boolean sismember(String s, String s1) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.sismember(s, s1);

	}

	@Override
	public String srandmember(String s) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.srandmember(s);

	}

	@Override
	public Long zadd(String s, double d, String s1) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.zadd(s, d, s1);

	}

	@Override
	public Long zadd(String s, Map map) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.zadd(s, map);

	}

	@Override
	public Set zrange(String s, long l, long l1) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.zrange(s, l, l1);

	}

	@Override
	public Long zrem(String s, String... as) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.zrem(s, as);

	}

	@Override
	public Double zincrby(String s, double d, String s1) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.zincrby(s, d, s1);

	}

	@Override
	public Long zrank(String s, String s1) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.zrank(s, s1);

	}

	@Override
	public Long zrevrank(String s, String s1) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.zrevrank(s, s1);

	}

	@Override
	public Set zrevrange(String s, long l, long l1) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.zrevrange(s, l, l1);

	}

	@Override
	public Set zrangeWithScores(String s, long l, long l1) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.zrangeWithScores(s, l, l1);

	}

	@Override
	public Set zrevrangeWithScores(String s, long l, long l1) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.zrevrangeWithScores(s, l, l1);

	}

	@Override
	public Long zcard(String s) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.zcard(s);

	}

	@Override
	public Double zscore(String s, String s1) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.zscore(s, s1);

	}

	@Override
	public List sort(String s) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.sort(s);

	}

	@Override
	public Long zcount(String s, double d, double d1) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.zcount(s, d, d1);

	}

	@Override
	public Long zcount(String s, String s1, String s2) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.zcount(s, s1, s2);

	}

	@Override
	public Set zrangeByScore(String s, double d, double d1) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.zrangeByScore(s, d, d1);

	}

	@Override
	public Set zrangeByScore(String s, String s1, String s2) {
		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.zrangeByScore(s, s1, s2);
	}

	@Override
	public Set zrevrangeByScore(String s, double d, double d1) {
		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.zrevrangeByScore(s, d, d1);
	}

	@Override
	public Set zrangeByScore(String s, double d, double d1, int i, int j) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.zrangeByScore(s, d, d1, i, j);

	}

	@Override
	public Set zrevrangeByScore(String s, String s1, String s2) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.zrevrangeByScore(s, s1, s2);

	}

	@Override
	public Set zrangeByScore(String s, String s1, String s2, int i, int j) {
		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.zrangeByScore(s, s1, s2, i, j);

	}

	@Override
	public Set zrevrangeByScore(String s, double d, double d1, int i, int j) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.zrevrangeByScore(s, d, d1, i, j);

	}

	@Override
	public Set zrangeByScoreWithScores(String s, double d, double d1) {
		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.zrangeByScoreWithScores(s, d, d1);
	}

	@Override
	public Set zrevrangeByScoreWithScores(String s, double d, double d1) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.zrevrangeByScoreWithScores(s, d, d1);
	}

	@Override
	public Set zrangeByScoreWithScores(String s, double d, double d1, int i,
			int j) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.zrangeByScoreWithScores(s, d, d1, i, j);

	}

	@Override
	public Set zrevrangeByScore(String s, String s1, String s2, int i, int j) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.zrevrangeByScore(s, s1, s2, i, j);

	}

	@Override
	public Set zrangeByScoreWithScores(String s, String s1, String s2) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.zrangeByScoreWithScores(s, s1, s2);

	}

	@Override
	public Set zrevrangeByScoreWithScores(String s, String s1, String s2) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.zrevrangeByScoreWithScores(s, s1, s2);

	}

	@Override
	public Set zrangeByScoreWithScores(String s, String s1, String s2, int i,
			int j) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.zrangeByScoreWithScores(s, s1, s2, i, j);

	}

	@Override
	public Set zrevrangeByScoreWithScores(String s, double d, double d1, int i,
			int j) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.zrevrangeByScoreWithScores(s, d, d1, i, j);

	}

	@Override
	public Set zrevrangeByScoreWithScores(String s, String s1, String s2,
			int i, int j) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.zrevrangeByScoreWithScores(s, s1, s2, i, j);

	}

	@Override
	public Long zremrangeByRank(String s, long l, long l1) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.zremrangeByRank(s, l, l1);

	}

	@Override
	public Long zremrangeByScore(String s, double d, double d1) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.zremrangeByScore(s, d, d1);

	}

	@Override
	public Long zremrangeByScore(String s, String s1, String s2) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.zremrangeByScore(s, s1, s2);

	}

	@Override
	public Long lpushx(String s, String s1) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.lpushx(s, s1);

	}

	@Override
	public Long rpushx(String s, String s1) {

		Redis4JClient client = (Redis4JClient) getClient(s);
		return client.rpushx(s, s1);

	}

	public static void main(String[] args) {
		int rehashTries = 0;
		String key = "safd";
		System.out.println(key.hashCode());
		String newKey = String.format("%s%s", rehashTries, key);
		System.out.println(newKey);
		System.out.println(newKey.hashCode());
	}
}
