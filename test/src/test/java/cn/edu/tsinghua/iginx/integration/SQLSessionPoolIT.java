package cn.edu.tsinghua.iginx.integration;

import org.junit.*;

public class SQLSessionPoolIT extends SQLSessionIT{

    @BeforeClass
    public static void beforeSetUp() {
        isForSessionPool = true;
        isForSession = false;
        MaxMultiThreadTaskNum = 10;
    }

}
