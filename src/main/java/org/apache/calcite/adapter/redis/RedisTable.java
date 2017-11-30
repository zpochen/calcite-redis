package org.apache.calcite.adapter.redis;


import com.google.common.base.Preconditions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;

import java.util.ArrayList;
import java.util.List;

public class RedisTable extends AbstractTable{

    private RedisTableDescription tableDescription;

    public RedisTable(RedisTableDescription tableDescription){
        this.tableDescription = tableDescription;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
        Preconditions.checkNotNull(tableDescription,"can't find table description.");

        List<RelDataType> types = new ArrayList<>();
        List<String> names = new ArrayList<>();

        RedisTableFieldGroup tableKeyFieldGroup = tableDescription.getKey();
        for (RedisTableFieldDescription : tableKeyFieldGroup.getFields()){

        }

        relDataTypeFactory.createSqlType()


        return null;
    }
}
