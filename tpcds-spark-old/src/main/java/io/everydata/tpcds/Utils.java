package io.everydata.tpcds;

import io.prestosql.tpcds.Options;
import io.prestosql.tpcds.Table;
import io.prestosql.tpcds.column.Column;
import io.prestosql.tpcds.column.ColumnType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Utils {
  private static final Logger logger = LoggerFactory.getLogger(Utils.class);

  public static StructType readSchema(String tableName) {
    // based on table name create schema and return
    Table table = Table.getTable(tableName.toLowerCase());
    Column[] columns = table.getColumns();

    List<StructField> collect = Arrays.asList(columns).stream().map(column -> {
      DataType dataType = getSparkDataType(column);
      return StructField
          .apply(column.getName(), dataType, true, Metadata.empty());
    }).collect(Collectors.toList());

    StructType structType = new StructType(collect.toArray(new StructField[0]));
    return structType;
  }

  private static DataType getSparkDataType(Column column) {
    ColumnType.Base baseType = column.getType().getBase();
    logger
        .info("CName -> {} | BaseType -> {} ", column.getName(), column.getType().getBase());
    DataType dataType = null;
    switch (baseType) {
      case INTEGER:
        dataType = DataTypes.LongType;
        break;
      case IDENTIFIER:
        dataType = DataTypes.LongType;
        break;
      case DATE:
        dataType = DataTypes.DateType;
        break;
      case DECIMAL:
        dataType = DataTypes.createDecimalType();
        break;
      case VARCHAR:
        dataType = DataTypes.StringType;
        break;
      case CHAR:
        dataType = DataTypes.StringType;
        break;
      case TIME:
        dataType = DataTypes.TimestampType;
        break;
    }

    logger.info("{} : {} ", column.getName(), dataType);
    return dataType;
  }

  public static Options asTpcdsOption(Map<String, String> allOptions) {
    Options options = new Options();
    options.setNoSexism(true);

    options.setParallelism(
        Integer.parseInt(allOptions.getOrDefault(TpcdsProperties.PARTITIONS, "1")));

    options
        .setScale(Double.parseDouble(allOptions.getOrDefault(TpcdsProperties.SCALE_FACTOR, "1")));

    return options;
  }
}
