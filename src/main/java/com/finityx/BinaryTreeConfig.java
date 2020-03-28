package com.finityx;

import com.finityx.common.BaseConfig;

import java.util.Properties;

public class BinaryTreeConfig extends BaseConfig {

    private final static String LOCAL_INPUT_FILE_FIELD = "finityx.local.input.file";
    private final static String SEARCHING_NODE_NAME_FIELD = "finityx.searching.node.name";

    private final String localInputFile;
    private final String searchingNodeName;

    BinaryTreeConfig(Properties properties) {
        super(properties);
        localInputFile = properties.getProperty(LOCAL_INPUT_FILE_FIELD);
        searchingNodeName = properties.getProperty(SEARCHING_NODE_NAME_FIELD);
    }


    public String getLocalFileField() {
        return localInputFile;
    }

    public String getSearchingNodeName() {
        return searchingNodeName;
    }
}
