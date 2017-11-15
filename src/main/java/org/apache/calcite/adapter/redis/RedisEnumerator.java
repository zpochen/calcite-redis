package org.apache.calcite.adapter.redis;

import org.apache.calcite.adapter.redis.client.RedisService;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import redis.clients.jedis.Jedis;

import java.util.Map;

public class RedisEnumerator implements Enumerator<Object[]> {
    private final Enumerator<Map.Entry<String, String>> enumerator;

    public RedisEnumerator(RedisService redisService,String table){
        try(Jedis redis = redisService.getResource()){
            Map<String,String> map = redis.hgetAll(table);
            enumerator = Linq4j.enumerator(map.entrySet());
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public Object[] current() {
        return new Object[]{enumerator.current()};
    }

    @Override
    public boolean moveNext() {
        return false;
    }

    @Override
    public void reset() {

    }

    @Override
    public void close() {

    }
}
