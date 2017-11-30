package org.apache.calcite.adapter.redis;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

public class RedisTableDescription {
    private final String tableName;
    private final RedisTableFieldGroup key;
    private final RedisTableFieldGroup value;

    @JsonCreator
    public RedisTableDescription(
            @JsonProperty("tableName") String tableName,
            @JsonProperty("key") RedisTableFieldGroup key,
            @JsonProperty("value") RedisTableFieldGroup value)
    {
        checkArgument(!isNullOrEmpty(tableName), "tableName is null or is empty");
        this.tableName = tableName;
        this.key = key;
        this.value = value;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public RedisTableFieldGroup getKey()
    {
        return key;
    }

    @JsonProperty
    public RedisTableFieldGroup getValue()
    {
        return value;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("tableName", tableName)
                .add("key", key)
                .add("value", value)
                .toString();
    }
}
