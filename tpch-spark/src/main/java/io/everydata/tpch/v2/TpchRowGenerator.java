package io.everydata.tpch.v2;

import io.everydata.tpch.Utils;
import io.prestosql.tpch.TpchEntity;
import io.prestosql.tpch.TpchTable;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.sources.v2.reader.DataReader;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class TpchRowGenerator
    implements DataReader<Row> {
  private static final Logger logger = LoggerFactory.getLogger(TpchRowGenerator.class);
  private final Map<String, String> allOptions;
  private final int designatedPartition;
  private Iterator<? extends TpchEntity> rowGenerator;

  private long servedRows = 0;
  private StructType schema;

  public TpchRowGenerator(Map<String, String> allOptions, int designatedPartition) {
    this.allOptions = allOptions;
    this.designatedPartition = designatedPartition;
    initializeTpchTableReader();
  }

  private void initializeTpchTableReader() {
    String tableName = TpchConfigHelper.getTableName(allOptions);
    int totalPartitions = TpchConfigHelper.getTablePartitions(allOptions);
    double scaleFactor = TpchConfigHelper.getScaleFactor(allOptions);
    logger.info(
        "Initializing Table Reader for Table {} with scale Factor {} for partition {} out of {} partitions",
        tableName, scaleFactor, designatedPartition, totalPartitions);
    TpchTable<? extends TpchEntity> table = TpchTable.getTable(tableName);
    rowGenerator =
        table.createGenerator(scaleFactor, designatedPartition, totalPartitions).iterator();
    schema = Utils.readSchema(tableName.toLowerCase());
  }

  @Override
  public boolean next()
      throws IOException {
    // has next row or not
    return rowGenerator.hasNext();
  }

  @Override
  public Row get() {
    // return tpch table row converted to spark row
    TpchEntity next = rowGenerator.next();
    servedRows++;
    Object[] values = next.values();
    Object[] convertedVals = new Object[values.length];

    for (int i = 0; i < values.length; i++) {
      convertedVals[i] = convertRead(schema.apply(i).dataType(), values[i]);
    }

    return new GenericRowWithSchema(convertedVals, schema);
  }

  @Override
  public void close()
      throws IOException {
    // close any of connection
    logger.info("Rows served {} ", servedRows);
  }

  public Object convertRead(DataType dt, Object value) {
    if (value == null) {
      return null;
    }

    if (dt instanceof StringType) {
      return String.valueOf(value);
        /*}
        else if(dt instanceof LongType){
            value
        }else if(dt instanceof IntegerType){

        }else if(dt instanceof BooleanType){

        }else if(dt instanceof DoubleType){

        }else if(dt instanceof FloatType){

        }else if(dt instanceof DateType){
*/
    } else {
      return value;
    }

  }
}
