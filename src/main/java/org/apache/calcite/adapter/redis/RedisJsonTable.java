package org.apache.calcite.adapter.redis;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.redis.client.RedisService;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
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
        /*return typeFactory.builder().add("_MAP",
                typeFactory.createMapType(
                        typeFactory.createSqlType(SqlTypeName.VARCHAR),
                        typeFactory.createTypeWithNullability(
                                typeFactory.createSqlType(SqlTypeName.VARCHAR), true))).build();*/
        RelDataType strType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR),true);

        return typeFactory.createStructType(ImmutableList.of(strType,strType,strType,strType),ImmutableList.of("id","name","symbol","rank"));

    }

    @Override
    public Enumerable<Object[]> scan(DataContext dataContext) {
        return new AbstractEnumerable<Object[]>() {
            public Enumerator<Object[]> enumerator() {
                return new RedisEnumerator(redisService,tableName);
            }
        };
    }



}
