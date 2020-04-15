package io.everydata.tpch;

import io.prestosql.tpch.TpchColumn;
import io.prestosql.tpch.TpchColumnType;
import io.prestosql.tpch.TpchTable;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

public class Utils {
  private static final Logger logger = LoggerFactory.getLogger(Utils.class);

  public static StructType readSchema(String tableName) {
    // based on table name create schema and return
    TpchTable<?> table = TpchTable.getTable(tableName.toLowerCase());
    List<? extends TpchColumn<?>> columns = table.getColumns();

    List<StructField> collect = columns.stream().map(column -> {
      DataType dataType = getSparkDataType(column);
      return StructField
          .apply(((TpchColumn) column).getColumnName(), dataType, true, Metadata.empty());
    }).collect(Collectors.toList());

    StructType structType = new StructType(collect.toArray(new StructField[0]));
    return structType;
  }

  private static DataType getSparkDataType(TpchColumn<?> column) {
    TpchColumnType.Base baseType = column.getType().getBase();
//    logger.info("CName -> {} | BaseType -> {} ", column.getColumnName(), column.getType().getBase());
    DataType dataType = null;
    switch (baseType) {
      case INTEGER:
        dataType = DataTypes.IntegerType;
        break;
      case IDENTIFIER:
        dataType = DataTypes.LongType;
        break;
      case DATE:
        dataType = DataTypes.DateType;
        break;
      case DOUBLE:
        dataType = DataTypes.DoubleType;
        break;
      case VARCHAR:
        dataType = DataTypes.StringType;
        break;
    }

    logger.debug("{} : Spark Data Type: {} ", column.getColumnName(), dataType);
    return dataType;
  }

}
