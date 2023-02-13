package cn.edu.tsinghua.iginx.iotdb;

import cn.edu.tsinghua.iginx.engine.physical.storage.fault_tolerance.Connector;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class IoTDBConnector implements Connector {

    private static final Logger logger = LoggerFactory.getLogger(IoTDBConnector.class);

    private static final ExecutorService service = Executors.newFixedThreadPool(20);

    private final long id;

    private Session session;

    public IoTDBConnector(long id, String ip, int port, String username, String password) {
        this.id = id;
        this.session = new Session(ip, port, username, password);
    }

    @Override
    public boolean echo(long timeout, TimeUnit unit) {
        //logger.info("[FaultTolerance][IoTDBConnector][id={}] echo start...", id);
        Future<Boolean> future = service.submit(() -> {
            try {
                session.open();
                SessionDataSet dataSet = session.executeQueryStatement("show version");
                dataSet.closeOperationHandle();
                session.close();
                //logger.info("[FaultTolerance][IoTDBConnector][id={}] echo connection success...", id);
            } catch (IoTDBConnectionException e) {
                logger.error("[FaultTolerance][IoTDBConnector][id={}] echo execute failure {}", id, e);
                return false;
            } catch (StatementExecutionException e) {
                logger.error("[FaultTolerance][IoTDBConnector][id={}] echo statement failure {}, but connection success", id, e);
            }
            return true;
        });
        try {
            return future.get(timeout, unit);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            future.cancel(true);
            logger.info("[FaultTolerance][IoTDBConnector][id={}] echo interrupt or timeout", id);
        }
        return false;
    }

    @Override
    public void reset() {
        this.session = null;
    }

}
