package io.everydata.tpch.v2;

import java.util.Map;

public class TpchConfigHelper
{
    public static String getTableName(Map<String, String> allOptions)
    {
        return allOptions.get(TpchProperties.TABLE_NAME);
    }

    public static int getTablePartitions(Map<String, String> allOptions)
    {
        return Integer.parseInt(allOptions.getOrDefault(TpchProperties.PARTITIONS, "1"));
    }

    public static double getScaleFactor(Map<String, String> allOptions)
    {
        return Double.parseDouble(allOptions.getOrDefault(TpchProperties.SCALE_FACTOR, "1"));
    }
}
