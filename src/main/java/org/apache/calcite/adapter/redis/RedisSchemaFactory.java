package org.apache.calcite.adapter.redis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class RedisSchemaFactory implements SchemaFactory{
    @Override
    public Schema create(SchemaPlus schemaPlus, String name, Map<String, Object> operand) {
        String redisHost = (String)operand.get("host");
        Integer redisPort = (Integer)operand.get("port");
        List tables = (List)operand.get("tables");

        RedisSchema schema = null;
        try {
            ObjectMapper mapper = new ObjectMapper();
            String schemaJson = mapper.writeValueAsString(operand);
            schema = mapper.readValue(schemaJson,RedisSchema.class);
            schema.setName(name);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("can't write operand to json.",e);
        } catch (IOException e) {
            throw new IllegalArgumentException("can't read json to schema.",e);
        }

        return schema;
    }
}
