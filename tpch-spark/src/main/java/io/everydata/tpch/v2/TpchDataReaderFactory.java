package io.everydata.tpch.v2;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.sources.v2.reader.DataReader;
import org.apache.spark.sql.sources.v2.reader.DataReaderFactory;

import java.util.Map;

public class TpchDataReaderFactory
        implements DataReaderFactory<Row>
{

    private final Map<String, String> allOptions;
    private final int designatedPartition;

    TpchDataReaderFactory(Map<String, String> allOptions, int designatedPartition)
    {
        // here parms should have split details
        // or tpch table details to create reader per partition or split
        this.allOptions = allOptions;
        this.designatedPartition = designatedPartition;
    }

    @Override
    public DataReader<Row> createDataReader()
    {
        return new TpchRowGenerator(this.allOptions, designatedPartition);
    }
}
