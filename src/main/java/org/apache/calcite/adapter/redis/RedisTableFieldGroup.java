package org.apache.calcite.adapter.redis;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

import java.util.List;

/**
 * Groups the field descriptions for value or key.
 */
public class RedisTableFieldGroup {
    private final String dataFormat;
    private final List<RedisTableFieldDescription> fields;

    @JsonCreator
    public RedisTableFieldGroup(
            @JsonProperty("dataFormat") String dataFormat,
            @JsonProperty("fields") List<RedisTableFieldDescription> fields)
    {
        this.dataFormat = requireNonNull(dataFormat, "dataFormat is null");
        this.fields = ImmutableList.copyOf(requireNonNull(fields, "fields is null"));
    }

    @JsonProperty
    public String getDataFormat()
    {
        return dataFormat;
    }


    @JsonProperty
    public List<RedisTableFieldDescription> getFields()
    {
        return fields;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("dataFormat", dataFormat)
                .add("fields", fields)
                .toString();
    }

}
