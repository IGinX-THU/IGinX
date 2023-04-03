package cn.edu.tsinghua.iginx.integration.func.sql;

import java.io.IOException;
import org.junit.*;

public class SQLSessionPoolIT extends SQLSessionIT {
    public SQLSessionPoolIT() throws IOException {
        super();
        isForSessionPool = true;
        isForSession = false;
        MaxMultiThreadTaskNum = 10;
    }
}
