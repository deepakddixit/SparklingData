package io.everydata.builder;

import io.everydata.config.Config;

import java.util.Properties;

public interface Builder
{
    Builder fromProperties(Properties properties);

    Config build();
}
