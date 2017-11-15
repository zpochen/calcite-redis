package org.apache.calcite.adapter.redis.client;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisServiceImpl implements RedisService {
	private String host;
	private int port;
	private JedisPool pool;

	public RedisServiceImpl(String host, int port) {
		this.host = host;
		this.port = port;
		GenericObjectPoolConfig config = new GenericObjectPoolConfig();
		config.setMaxWaitMillis(5000);
		pool = new JedisPool(config, this.host, this.port);
	}

	@Override
	public Jedis getResource() {
		return pool.getResource();
	}

	@Override
	public void close(Jedis jedis) {
		try {
			if (jedis != null) {
				jedis.close();
			}
		} catch (Exception e) {
			//igonre
		}

	}

	@Override
	public void destory() {
		if (pool != null)
			pool.destroy();
	}

}
