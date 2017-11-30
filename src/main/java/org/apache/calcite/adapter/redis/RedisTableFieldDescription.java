package org.apache.calcite.adapter.redis;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

/**
 * Json description to parse a single field from a Redis key/value row.
 *
 * */
public class RedisTableFieldDescription {
    private final String name;
    private final SqlTypeName type;
    private final String mapping;
    private final String comment;

    @JsonCreator
    public RedisTableFieldDescription(
            @JsonProperty("name") String name,
            @JsonProperty("type") SqlTypeName type,
            @JsonProperty("mapping") String mapping,
            @JsonProperty("comment") String comment)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or is empty");
        this.name = name;
        this.type = requireNonNull(type, "type is null");
        this.mapping = mapping;
        this.comment = comment;
    }

    @JsonProperty("name")
    public String getName() {
        return name;
    }

    @JsonProperty("type")
    public SqlTypeName getType() {
        return type;
    }

    @JsonProperty("mapping")
    public String getMapping() {
        return mapping;
    }

    @JsonProperty("comment")
    public String getComment() {
        return comment;
    }
}
