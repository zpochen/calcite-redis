package org.apache.calcite.adapter.redis.client;

import redis.clients.jedis.Jedis;

public interface RedisService {
	
	Jedis getResource();
	void close(Jedis jedis);
	void destory();
	
}
