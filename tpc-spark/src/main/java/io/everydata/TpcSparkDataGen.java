package io.everydata;

import io.everydata.config.TpcDataGenConfig;
import io.prestosql.tpch.TpchColumn;
import io.prestosql.tpch.TpchColumnType;
import io.prestosql.tpch.TpchTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringReader;
import java.util.List;
import java.util.Properties;

public class TpcSparkDataGen
{
    private static final Logger logger = LoggerFactory.getLogger(TpcSparkApp.class);

    public static void main(String[] args)
            throws DataGenException
    {
        try {
            if (args == null || (args.length == 0)) {
                throw new IllegalArgumentException(
                        "Missing mandatory argument, all properties necessary for job.");
            }

            Properties properties = new Properties();
            properties.load(new StringReader(args[0]));

            logger.info("Received Job Properties {}", properties);

            //---------------------------------

            TpcDataGenConfig.TpcDataGenConfigBuilder
                    builder =
                    new TpcDataGenConfig.TpcDataGenConfigBuilder();
            builder.fromProperties(properties);
            TpcDataGenConfig config = builder.build();

            List<TpchTable<?>> tpchTableList = TpchTable.getTables();
            tpchTableList.forEach(tpchTable -> {
                List<? extends TpchColumn<?>> columns = tpchTable.getColumns();
                columns.forEach(col -> {
                    TpchColumnType type = col.getType();
                });
            });
        }
        catch (Throwable t) {
            throw new DataGenException(t.getMessage(), t);
        }
    }
}
