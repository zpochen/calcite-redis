package org.apache.calcite.adapter.redis;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.List;
import java.util.Map;

public class RedisSchemaFactory implements SchemaFactory{
    @Override
    public Schema create(SchemaPlus schemaPlus, String s, Map<String, Object> operand) {
        String redisHost = (String)operand.get("host");
        Integer redisPort = (Integer)operand.get("port");
        List<String> hashKeys = (List)operand.get("tables");

        return new RedisSchema(redisHost,redisPort,hashKeys);
    }
}
