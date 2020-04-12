package io.everydata.sinks;

public enum SinkType
{
    FILE(FileSink.class);

    private final Class<FileSink> sinkClass;

    SinkType(Class<FileSink> sinkClass)
    {
        this.sinkClass = sinkClass;
    }
}
