package org.jinn.redis;

import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract interface Redis4JCommand {
	
	    public abstract boolean isAlive();
	   
	    public abstract String flushDB();
	  
	    public abstract String ping();

	    public abstract String set(String s, String s1);

		public abstract String get(String s);

		public abstract Boolean exists(String s);

		public abstract String type(String s);

		public abstract Long expire(String s, int i);

		public abstract Long expireAt(String s, long l);

		public abstract Long ttl(String s);

//		public abstract Boolean setbit(String s, long l, boolean flag);
//
//		public abstract Boolean getbit(String s, long l);

		public abstract Long setrange(String s, long l, String s1);

		public abstract String getrange(String s, long l, long l1);

		public abstract String getSet(String s, String s1);

		public abstract Long setnx(String s, String s1);

		public abstract String setex(String s, int i, String s1);

		public abstract Long decrBy(String s, long l);

		public abstract Long decr(String s);

		public abstract Long incrBy(String s, long l);

		public abstract Long incr(String s);

		public abstract Long append(String s, String s1);

		public abstract String substr(String s, int i, int j);

		public abstract Long hset(String s, String s1, String s2);

		public abstract String hget(String s, String s1);

		public abstract Long hsetnx(String s, String s1, String s2);

		public abstract String hmset(String s, Map map);

		public abstract List hmget(String s, String... as);

		public abstract Long hincrBy(String s, String s1, long l);

		public abstract Boolean hexists(String s, String s1);

		public abstract Long hdel(String s, String... as);

		public abstract Long hlen(String s);

		public abstract Set hkeys(String s);

		public abstract List hvals(String s);

		public abstract Map hgetAll(String s);

		public abstract Long rpush(String s, String... as);

		public abstract Long lpush(String s, String... as);

		public abstract Long llen(String s);

		public abstract List lrange(String s, long l, long l1);

		public abstract String ltrim(String s, long l, long l1);

		public abstract String lindex(String s, long l);

		public abstract String lset(String s, long l, String s1);

		public abstract Long lrem(String s, long l, String s1);

		public abstract String lpop(String s);

		public abstract String rpop(String s);

		public abstract Long sadd(String s, String... as);

		public abstract Set smembers(String s);

		public abstract Long srem(String s, String... as);

		public abstract String spop(String s);

		public abstract Long scard(String s);

		public abstract Boolean sismember(String s, String s1);

		public abstract String srandmember(String s);

		public abstract Long zadd(String s, double d, String s1);

		public abstract Long zadd(String s, Map map);

		public abstract Set zrange(String s, long l, long l1);

		public abstract Long zrem(String s, String... as);

		public abstract Double zincrby(String s, double d, String s1);

		public abstract Long zrank(String s, String s1);

		public abstract Long zrevrank(String s, String s1);

		public abstract Set zrevrange(String s, long l, long l1);

		public abstract Set zrangeWithScores(String s, long l, long l1);

		public abstract Set zrevrangeWithScores(String s, long l, long l1);

		public abstract Long zcard(String s);

		public abstract Double zscore(String s, String s1);

		public abstract List sort(String s);

		public abstract Long zcount(String s, double d, double d1);

		public abstract Long zcount(String s, String s1, String s2);

		public abstract Set zrangeByScore(String s, double d, double d1);

		public abstract Set zrangeByScore(String s, String s1, String s2);

		public abstract Set zrevrangeByScore(String s, double d, double d1);

		public abstract Set zrangeByScore(String s, double d, double d1, int i,
				int j);

		public abstract Set zrevrangeByScore(String s, String s1, String s2);

		public abstract Set zrangeByScore(String s, String s1, String s2, int i,
				int j);

		public abstract Set zrevrangeByScore(String s, double d, double d1, int i,
				int j);

		public abstract Set zrangeByScoreWithScores(String s, double d, double d1);

		public abstract Set zrevrangeByScoreWithScores(String s, double d, double d1);

		public abstract Set zrangeByScoreWithScores(String s, double d, double d1,
				int i, int j);

		public abstract Set zrevrangeByScore(String s, String s1, String s2, int i,
				int j);

		public abstract Set zrangeByScoreWithScores(String s, String s1, String s2);

		public abstract Set zrevrangeByScoreWithScores(String s, String s1,
				String s2);

		public abstract Set zrangeByScoreWithScores(String s, String s1, String s2,
				int i, int j);

		public abstract Set zrevrangeByScoreWithScores(String s, double d,
				double d1, int i, int j);

		public abstract Set zrevrangeByScoreWithScores(String s, String s1,
				String s2, int i, int j);

		public abstract Long zremrangeByRank(String s, long l, long l1);

		public abstract Long zremrangeByScore(String s, double d, double d1);

		public abstract Long zremrangeByScore(String s, String s1, String s2);

		public abstract Long lpushx(String s, String s1);

		public abstract Long rpushx(String s, String s1);
}
