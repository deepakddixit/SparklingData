package io.everydata.tpch.v2;

import io.everydata.tpch.Utils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.sources.v2.reader.DataReaderFactory;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class TpchTableReader
    implements DataSourceReader {
  private static final Logger logger = LoggerFactory.getLogger(TpchTableReader.class);
  private final String tableName;
  private final int partitionCount;
  private Map<String, String> allOptions;

  TpchTableReader(Map<String, String> allOptions) {
    this.allOptions = allOptions;
    if (!allOptions.containsKey(TpchProperties.TABLE_NAME)) {
      logger.error("No table name configured. Throwing exception");
      String
          msg =
          String
              .format("No table name configured. Specify property [%s]", TpchProperties.TABLE_NAME);
      throw new io.everydata.tpch.exception.TpchRunTimeException(msg);
    }

    if (!allOptions.containsKey(TpchProperties.SCALE_FACTOR)) {
      logger.error("No scale factor configured. Throwing exception");
      String
          msg =
          String
              .format("No scale factor configured. Specify property [%s]",
                  TpchProperties.SCALE_FACTOR);
      throw new io.everydata.tpch.exception.TpchRunTimeException(msg);
    }
    this.tableName = TpchConfigHelper.getTableName(allOptions);

    if (!allOptions.containsKey(TpchProperties.PARTITIONS)) {
      logger.info("Partitions count not provided so will be using single partition");
    }
    partitionCount = TpchConfigHelper.getTablePartitions(allOptions);
  }

  @Override
  public StructType readSchema() {
    // based on table name create schema and return
    logger.trace("In the readSchema");
    return Utils.readSchema(tableName.toLowerCase());
  }

  @Override
  public List<DataReaderFactory<Row>> createDataReaderFactories() {
    // here we should create per partition one data reader factory
    // e.g 10 parts
    List<DataReaderFactory<Row>> factories = new LinkedList<>();
    for (int i = 1; i <= partitionCount; i++) {
      factories.add(new TpchDataReaderFactory(allOptions, i));
    }
    return factories;
  }
}
