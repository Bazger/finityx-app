package com.finityx.common;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class Application<TContext extends StreamingContextProvider> {

    private SparkStreamingContext context;

    public void run(TContext context, String[] args) throws Exception {

        String appName = getMandatoryField(args, "app_name");
        String mode = getMandatoryField(args, "mode");
        String configPath = getMandatoryField(args, "config_path");
        String master = getOptionalField(args, "master", "local[*]");

        this.context = context.create(appName, master, configPath);

        modeSelection(mode, args);
    }

    private void modeSelection(String mode, String[] args) {
        switch (mode) {
            case "batch": //Running spark batch
                doBatch(args);
                break;
            case "streaming": //Running spark streaming
                throw new NotImplementedException();
        }
    }

    private void doBatch(String[] args) {
        this.context.process();
    }


    private String getMandatoryField(String[] args, String field) throws Exception {
        for (String arg : args) {
            String[] splitArg = arg.split("=");
            if (splitArg[0].equals(field)) {
                return splitArg[1];
            }
        }
        throw new Exception(String.format("Command argument %s is missing", field));
    }

    private String getOptionalField(String[] args, String field, String defaultValue) throws Exception {
        for (String arg : args) {
            String[] splitArg = arg.split("=");
            if (splitArg[0].equals(field)) {
                return splitArg[1];
            }
        }
        return defaultValue;
    }
}
