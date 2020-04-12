package io.everydata.tpch.v2;

import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TpchDataSource
        implements DataSourceV2, ReadSupport
{
    private static final Logger logger = LoggerFactory.getLogger(TpchDataSource.class);

    @Override
    public DataSourceReader createReader(DataSourceOptions dataSourceOptions)
    {
        // these should have all options
        logger.info("Options received {}", dataSourceOptions.asMap());
        return new TpchTableReader(dataSourceOptions.asMap());
    }
}
