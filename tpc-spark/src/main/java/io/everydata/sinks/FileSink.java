package io.everydata.sinks;

public class FileSink
        implements Sink
{
    @Override
    public SinkType getSinkType()
    {
        return SinkType.FILE;
    }

    @Override
    public SinkConfig getSinkConfig()
    {
        return null;
    }
}
