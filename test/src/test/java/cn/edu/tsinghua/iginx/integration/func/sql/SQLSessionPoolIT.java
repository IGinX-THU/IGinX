package cn.edu.tsinghua.iginx.integration.func.sql;

import org.junit.*;

import java.io.IOException;

public class SQLSessionPoolIT extends SQLSessionIT {
    public SQLSessionPoolIT() throws IOException {
        super();
        isForSessionPool = true;
        isForSession = false;
        MaxMultiThreadTaskNum = 10;
    }
}
