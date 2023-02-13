package cn.edu.tsinghua.iginx.influxdb;

import cn.edu.tsinghua.iginx.engine.physical.storage.fault_tolerance.Connector;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class InfluxDBConnector implements Connector {

    private static final Logger logger = LoggerFactory.getLogger(InfluxDBConnector.class);

    private static final ExecutorService service = Executors.newFixedThreadPool(20);

    private final long id;

    private final String url;

    private final char[] token;

    public InfluxDBConnector(long id, String url, char[] token) {
        this.id = id;
        this.url = url;
        this.token = token;
    }

    @Override
    public boolean echo(long timeout, TimeUnit unit) {
        //logger.info("[FaultTolerance][InfluxDBConnector][id={}] in echo", id);
        Future<Boolean> future = service.submit(() -> {
            try (InfluxDBClient client = InfluxDBClientFactory.create(url, token)) {
                client.health().getVersion();
                //logger.info("[FaultTolerance][InfluxDBConnector][id={}] echo connection success, version={}", id, client.health().getVersion());
            } catch (Exception e) {
                logger.error("[FaultTolerance][InfluxDBConnector][id={}] echo statement failure {}", id, e);
                return false;
            }
            return true;
        });
        try {
            return future.get(timeout, unit);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            future.cancel(true);
            logger.info("[FaultTolerance][InfluxDBConnector][id={}] echo interrupt or timeout", id);
        }
        return false;
    }

    @Override
    public void reset() {

    }
}
