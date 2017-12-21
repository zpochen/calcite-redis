package org.apache.calcite.adapter.redis;

import com.google.common.base.Preconditions;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;

public class RedisTable extends AbstractTable implements ScannableTable{

    private RedisTableDescription tableDescription;

    public RedisTable(RedisTableDescription tableDescription){
        this.tableDescription = tableDescription;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
        Preconditions.checkNotNull(tableDescription,"can't find table description.");

        List<RelDataType> types = new ArrayList<>();
        List<String> names = new ArrayList<>();

        RedisTableFieldGroup tableKeyFieldGroup = tableDescription.getValue();
        for (RedisTableFieldDescription fieldDescription: tableKeyFieldGroup.getFields()){
            RelDataType type = relDataTypeFactory.createSqlType(fieldDescription.getType());
            String name = fieldDescription.getName();
            types.add(type);
            names.add(name);
        }

        return relDataTypeFactory.createStructType(Pair.zip(names,types));
    }

    @Override
    public Enumerable<Object[]> scan(DataContext dataContext) {
        return null;
    }
}
