package org.apache.calcite.adapter.redis;

import org.apache.calcite.adapter.redis.client.RedisService;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;

public abstract class RedisTable extends AbstractTable {
    private RedisService redisService;
    private String tableName;

    public RedisTable(RedisService redisService,String hashKey){
        this.redisService = redisService;
        /** hash key as the table name */
        this.tableName = hashKey;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
        return null;
    }
}
