package org.apache.calcite.adapter.redis;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.adapter.redis.client.RedisService;
import org.apache.calcite.adapter.redis.client.RedisServiceImpl;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.List;
import java.util.Map;

public class RedisSchema extends AbstractSchema {
    private String host;
    private int port;
    private List<String> hashTableKeys;
    private RedisService redisService;
    private Map<String, Table> tableMap;

    public RedisSchema(String host, int port,List<String> hashTableKeys) {
        this.host = host;
        this.port = port;
        this.hashTableKeys = hashTableKeys;
        redisService = new RedisServiceImpl(host,port);
    }

    @Override
    protected Map<String, Table> getTableMap() {
        if (tableMap == null){
            tableMap = createTableMap();
        }
        return tableMap;
    }

    private Map<String,Table> createTableMap(){
        final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();

        if (hashTableKeys == null || hashTableKeys.isEmpty()){
            return builder.build();
        }

        for (String key : hashTableKeys){
            builder.put(key, new RedisScannableTable(redisService,key) );
        }
        return builder.build();
    }
}
