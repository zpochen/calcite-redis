package org.apache.calcite.test;

import org.apache.calcite.adapter.redis.client.RedisService;
import org.apache.calcite.adapter.redis.client.RedisServiceImpl;
import redis.clients.jedis.Jedis;

public class DataUtil {
    public static void main(String[] args){
        RedisService redisService = new RedisServiceImpl("192.168.36.104",6379);
        Jedis redis = redisService.getResource();
        redis.hset("POPO","1","{\n" +
                "        \"id\": \"bitcoin\", \n" +
                "        \"name\": \"Bitcoin\", \n" +
                "        \"symbol\": \"BTC\", \n" +
                "        \"rank\": \"1\"\n" +
                "        \n" +
                "    }");

        redis.hset("POPO","2","{\n" +
                "        \"id\": \"ethereum\", \n" +
                "        \"name\": \"Ethereum\", \n" +
                "        \"symbol\": \"ETH\", \n" +
                "        \"rank\": \"2\"\n" +
                "    }");
    }
}
