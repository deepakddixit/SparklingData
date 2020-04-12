package io.everydata.tpch.v2;

import io.everydata.tpch.v2.exception.TpchRunTimeException;
import io.prestosql.tpch.TpchColumn;
import io.prestosql.tpch.TpchColumnType;
import io.prestosql.tpch.TpchTable;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.sources.v2.reader.DataReaderFactory;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TpchTableReader
        implements DataSourceReader
{
    private static final Logger logger = LoggerFactory.getLogger(TpchTableReader.class);
    private final String tableName;
    private final int partitionCount;
    private Map<String, String> allOptions;

    TpchTableReader(Map<String, String> allOptions)
    {
        this.allOptions = allOptions;
        if (!allOptions.containsKey(TpchProperties.TABLE_NAME)) {
            logger.error("No table name configured. Throwing exception");
            String
                    msg =
                    String
                            .format("No table name configured. Specify property [%s]", TpchProperties.TABLE_NAME);
            throw new TpchRunTimeException(msg);
        }

        if (!allOptions.containsKey(TpchProperties.SCALE_FACTOR)) {
            logger.error("No scale factor configured. Throwing exception");
            String
                    msg =
                    String
                            .format("No scale factor configured. Specify property [%s]",
                                    TpchProperties.SCALE_FACTOR);
            throw new TpchRunTimeException(msg);
        }
        this.tableName = TpchConfigHelper.getTableName(allOptions);

        if (!allOptions.containsKey(TpchProperties.PARTITIONS)) {
            logger.info("Partitions count not provided so will be using single partition");
        }
        partitionCount = TpchConfigHelper.getTablePartitions(allOptions);
    }

    @Override
    public StructType readSchema()
    {
        // based on table name create schema and return
        logger.trace("In the readSchema");
        TpchTable<?> table = TpchTable.getTable(tableName.toLowerCase());
        List<? extends TpchColumn<?>> columns = table.getColumns();
        logger.info("TpchCols {} ", columns);

        List<StructField> collect = columns.stream().map(column -> {
            DataType dataType = getSparkDataType(column);
            return StructField
                    .apply(((TpchColumn) column).getColumnName(), dataType, false, Metadata.empty());
        }).collect(Collectors.toList());

        StructType structType = new StructType(collect.toArray(new StructField[0]));
        logger.info("Final struct {} ", structType);
        return structType;
    }

    private DataType getSparkDataType(TpchColumn<?> column)
    {
        TpchColumnType.Base baseType = column.getType().getBase();

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
        logger.info("{} : {} ", column.getColumnName(), dataType);
        return dataType;
    }

    @Override
    public List<DataReaderFactory<Row>> createDataReaderFactories()
    {
        // here we should create per partition one data reader factory
        // e.g 10 parts
        List<DataReaderFactory<Row>> factories = new LinkedList<>();
        for (int i = 1; i <= partitionCount; i++) {
            factories.add(new TpchDataReaderFactory(allOptions, i));
        }
        return factories;
    }
}
