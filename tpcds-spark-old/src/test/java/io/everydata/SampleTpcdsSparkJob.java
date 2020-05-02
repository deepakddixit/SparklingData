package io.everydata;

import io.everydata.tpcds.TpcdsProperties;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SampleTpcdsSparkJob {

  public static void main(String[] args) {
    SparkSession
        testSpark =
        SparkSession.builder().appName("TestSpark").master("local[*]").getOrCreate();

    Dataset<Row>
        region =
        testSpark.read().format("io.everydata.tpcds.TpcdsDataSource")
            .option(TpcdsProperties.TABLE_NAME, "call_center")
            .option(TpcdsProperties.SCALE_FACTOR, "1.0").option(TpcdsProperties.PARTITIONS, "5")
            .load();
    region.printSchema();
    region.show();

//    SparkSession
//        testSpark =
//        SparkSession.builder().appName("TestSpark").master("local[*]").getOrCreate();

    /*List<String>
        tpch_table_names =
        TpchTable.getTables().stream().map(TpchTable::getTableName)
            .collect(Collectors.toList());

    String DATA_SOURCE_FORMAT = "io.everydata.tpch.TpchDataSource";

    tpch_table_names.forEach(tname -> {
      Dataset<Row>
          region =
          testSpark.read().format(DATA_SOURCE_FORMAT)
              .option(TpcdsProperties.TABLE_NAME, tname.trim())
              .option(TpcdsProperties.SCALE_FACTOR, "1").option(TpcdsProperties.PARTITIONS, "5")
              .load();
      region.printSchema();
      region.show();
    });*/
  }
}
