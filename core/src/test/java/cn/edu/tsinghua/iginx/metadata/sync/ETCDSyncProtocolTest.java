package cn.edu.tsinghua.iginx.metadata.sync;

import cn.edu.tsinghua.iginx.metadata.sync.protocol.SyncProtocol;
import cn.edu.tsinghua.iginx.metadata.sync.protocol.etcd.ETCDSyncProtocolImpl;
import io.etcd.jetcd.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ETCDSyncProtocolTest extends SyncProtocolTest {
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(ETCDSyncProtocolTest.class);

    public static final String END_POINTS = "http://localhost:2379";

    @Override
    protected SyncProtocol newSyncProtocol(String category) {
        return new ETCDSyncProtocolImpl(category, Client.builder()
                .endpoints(END_POINTS.split(","))
                .build());
    }
}
