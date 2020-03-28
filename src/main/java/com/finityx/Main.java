package com.finityx;


import com.finityx.common.Application;

public class Main {

    //For running this app add args to java command line
    //Example: app_name=FinityXApp config_path=.\env\app.properties mode=batch
    public static void main(String[] args) throws Exception {
        new Application<>().run(BinaryTreeStreamingContext::new, args);
    }
}
