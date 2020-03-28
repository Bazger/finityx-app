package com.finityx.common;

import java.io.IOException;

public interface StreamingContextProvider {
    SparkStreamingContext create(String appName, String master, String configPath) throws IOException;
}
