package io.everydata.sinks;

public interface Sink
{
    SinkType getSinkType();

    SinkConfig getSinkConfig();
}
