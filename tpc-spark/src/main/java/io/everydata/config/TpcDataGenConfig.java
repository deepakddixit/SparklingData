package io.everydata.config;

import io.everydata.builder.Builder;
import io.everydata.dataset.DataSetType;
import io.everydata.sinks.SinkType;

import java.util.Properties;

public class TpcDataGenConfig
        implements Config
{
    private DataSetType dataSet;
    private int scaleFactor;

    private SinkType sink;

    // use builder
    private TpcDataGenConfig()
    {

    }

    public DataSetType getDataSet()
    {
        return dataSet;
    }

    public void setDataSet(DataSetType dataSet)
    {
        this.dataSet = dataSet;
    }

    public int getScaleFactor()
    {
        return scaleFactor;
    }

    public void setScaleFactor(int scaleFactor)
    {
        this.scaleFactor = scaleFactor;
    }

    public SinkType getSink()
    {
        return sink;
    }

    public void setSink(SinkType sink)
    {
        this.sink = sink;
    }

    public Builder getBuilder()
    {
        return new TpcDataGenConfigBuilder();
    }

    public static class TpcDataGenConfigBuilder
            implements Builder
    {

        private final TpcDataGenConfig config;

        public TpcDataGenConfigBuilder()
        {
            this.config = new TpcDataGenConfig();
        }

        public TpcDataGenConfigBuilder withDataSet(DataSetType dataSet)
        {
            this.config.setDataSet(dataSet);
            return this;
        }

        public TpcDataGenConfigBuilder withSink(SinkType sink)
        {
            this.config.setSink(sink);
            return this;
        }

        public TpcDataGenConfigBuilder fromProperties(Properties allProperties)
        {

            return this;
        }

        public TpcDataGenConfig build()
        {
            // validate

            return config;
        }
    }
}
