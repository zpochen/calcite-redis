package org.apache.calcite.adapter.redis;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.adapter.redis.client.RedisService;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RedisEnumerator implements Enumerator<Object[]> {
    private final Enumerator<Object> enumerator;

    public RedisEnumerator(RedisService redisService,String table){
        Jedis redis = redisService.getResource();
        List<String> vals = redis.hvals(table);
        if (vals == null){
            vals = new ArrayList<>(0);
        }
        List<Object> jsonObjects = new ArrayList<>(vals.size());

        final ObjectMapper mapper = new ObjectMapper();
        mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        mapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
        try{
            for (String val : vals){
                Object o = mapper.readValue(val,Object.class);
                jsonObjects.add(o);
            }
        }catch (IOException e){
            throw new RuntimeException(e);
        }
        enumerator = Linq4j.enumerator(jsonObjects);
    }

    @Override
    public Object[] current() {
       // return new Object[]{enumerator.current()};
        return new Object[]{"id","name","symbol","rank"};
    }

    @Override
    public boolean moveNext() {
        return enumerator.moveNext();
    }

    @Override
    public void reset() {
        enumerator.reset();
    }

    @Override
    public void close() {
        enumerator.close();
    }
}
