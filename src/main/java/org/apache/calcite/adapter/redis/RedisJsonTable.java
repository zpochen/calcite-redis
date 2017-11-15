package org.apache.calcite.adapter.redis;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.redis.client.RedisService;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

public class RedisJsonTable extends AbstractTable implements ScannableTable {

    private RedisService redisService;
    private String tableName;

    public RedisJsonTable(RedisService redisService, String hashKey) {
        this.redisService = redisService;
        /** hash key as the table name */
        this.tableName = hashKey;
    }

    public String toString() {
        return "JsonTable";
    }

    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.builder().add("_MAP",
                typeFactory.createMapType(
                        typeFactory.createSqlType(SqlTypeName.VARCHAR),
                        typeFactory.createTypeWithNullability(
                                typeFactory.createSqlType(SqlTypeName.VARCHAR), true))).build();
    }

    @Override
    public Enumerable<Object[]> scan(DataContext dataContext) {
        return null;
    }



}
