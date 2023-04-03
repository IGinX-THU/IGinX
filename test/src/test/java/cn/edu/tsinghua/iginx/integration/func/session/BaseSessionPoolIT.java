package cn.edu.tsinghua.iginx.integration.func.session;

import cn.edu.tsinghua.iginx.integration.tool.MultiConnection;
import cn.edu.tsinghua.iginx.pool.SessionPool;
import org.junit.Before;

public class BaseSessionPoolIT extends BaseSessionIT {

    // check in BaseSessionIT for the initializations of MultiThreadTask before assignment
    private final int MaxMultiThreadTaskNum = 10;

    @Before
    @Override
    public void setUp() {
        try {
            session =
                    new MultiConnection(
                            new SessionPool.Builder()
                                    .host(defaultTestHost)
                                    .port(defaultTestPort)
                                    .user(defaultTestUser)
                                    .password(defaultTestPass)
                                    .maxSize(MaxMultiThreadTaskNum)
                                    .build());
            session.openSession();
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }
}
