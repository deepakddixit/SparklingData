package io.everydata;

import io.everydata.tpch.TpchProperties;
import io.prestosql.tpch.TpchTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.stream.Collectors;

public class SampleSparkJob {

  public static void main(String[] args) {
        /*SparkSession
                testSpark =
                SparkSession.builder().appName("TestSpark").master("local[*]").getOrCreate();

        Dataset<Row>
                region =
                testSpark.read().format("io.everydata.tpch.TpchDataSource")
                        .option(TpchProperties.TABLE_NAME, "supplier")
                        .option(TpchProperties.SCALE_FACTOR, "1.0").option(TpchProperties.PARTITIONS, "5").load();
        region.printSchema();
        region.show();*/

    SparkSession
        testSpark =
        SparkSession.builder().appName("TestSpark").master("local[*]").getOrCreate();

    List<String>
        tpch_table_names =
        TpchTable.getTables().stream().map(TpchTable::getTableName)
            .collect(Collectors.toList());

    String DATA_SOURCE_FORMAT = "io.everydata.tpch.TpchDataSource";

    tpch_table_names.forEach(tname -> {
      Dataset<Row>
          region =
          testSpark.read().format(DATA_SOURCE_FORMAT)
              .option(TpchProperties.TABLE_NAME, tname.trim())
              .option(TpchProperties.SCALE_FACTOR, "1").option(TpchProperties.PARTITIONS, "5")
              .load();
      region.printSchema();
      region.show();
    });
  }
}
