package com.finityx.common;

import java.util.Properties;

public interface BaseConfigProvider<TConfig extends BaseConfig> {
    TConfig create(Properties properties);
}
