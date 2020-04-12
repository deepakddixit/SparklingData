package io.everydata;

import io.everydata.tpch.v2.TpchProperties;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SampleSparkJob
{

    public static void main(String[] args)
    {
        SparkSession
                testSpark =
                SparkSession.builder().appName("TestSpark").master("local[*]").getOrCreate();

        Dataset<Row>
                region =
                testSpark.read().format("io.everydata.tpch.v2.TpchDataSource")
                        .option(TpchProperties.TABLE_NAME, "customer")
                        .option(TpchProperties.SCALE_FACTOR, "1").option(TpchProperties.PARTITIONS, "5").load();
        region.printSchema();
        region.show();
    }
}
