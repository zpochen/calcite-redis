package org.apache.calcite.adapter.redis;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RedisSchema extends AbstractSchema {
    private String name;
    private String host;
    private int port;
    private List<RedisTableDescription> tables;
    private Map<String, Table> tableMap;

    @JsonCreator
    public RedisSchema(@JsonProperty("host") String host,
                       @JsonProperty("port") int port,
                       @JsonProperty("tables") List<RedisTableDescription> tables) {
        this.host = host;
        this.port = port;
        this.tables = tables;
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

        if (tables == null || tables.isEmpty()){
            return builder.build();
        }

        for (RedisTableDescription table : tables){
            builder.put(table.getTableName(), new RedisTable(table) );
        }
        return builder.build();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @JsonProperty
    public String getHost() {
        return host;
    }

    @JsonProperty
    public int getPort() {
        return port;
    }

    @JsonProperty
    public List<RedisTableDescription> getTables() {
        return tables;
    }
}
