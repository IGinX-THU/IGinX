package cn.edu.tsinghua.iginx.fault_tolerance;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.thrift.*;
import cn.edu.tsinghua.iginx.utils.RpcUtils;
import org.apache.thrift.TConfiguration;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Proxy;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public final class IGinXSession {

    private static final Logger logger = LoggerFactory.getLogger(IGinXSession.class);

    private static final TimePrecision timeUnit = TimePrecision.NS;

    private static final long RETRY_INTERVAL_MS = 100;

    private IService.Iface client;

    private TTransport transport;

    private long sessionId;

    private boolean isClosed;

    private final IGinXSessionSettings settings;

    private IGinXEndPoint currEndPoint;

    private ScheduledExecutorService autoLoadService;

    public IGinXSession(IGinXSessionSettings settings) {
        if (settings == null) {
            throw new IllegalArgumentException("settings is null");
        }
        this.settings = settings;
        this.sessionId = -1L;
        this.isClosed = true;
        this.autoLoadService = null;
        this.currEndPoint = null;
    }

    public synchronized void open() throws SessionException {
        if (!isClosed) {
            return;
        }
        boolean flag = false;
        for (int i = 0; i < settings.getMaxSwitchTimes(); i++) {
            if (!settings.hasMoreEndPoint()) {
                break;
            }
            IGinXEndPoint endPoint = settings.getEndPoint();
            try {
                this.currEndPoint = endPoint;
                open(endPoint);
                flag = true;
                break;
            } catch (SessionException e) {
                logger.error("connect to " + endPoint + " error: ", e);
            } catch (ExecutionException e) {
                throw new SessionException(e);
            }
        }
        if (!flag) {
            throw new SessionException("No IGinX available, please check network.");
        }
        settings.resetEndPointsIndex();

        if (settings.isEnableHighAvailable() && settings.isAutoLoadAvailableList()) {
            autoLoadService = Executors.newSingleThreadScheduledExecutor();
            autoLoadService.scheduleWithFixedDelay(() -> {
                LoadAvailableEndPointsReq req = new LoadAvailableEndPointsReq(sessionId);
                req.setSize(settings.getMaxSwitchTimes() + 1);
                try {
                    LoadAvailableEndPointsResp resp = client.loadAvailableEndPoints(req);
                    if (resp.getEndPoints() != null) {
                        settings.setEndPoints(resp.getEndPoints());
                    }
                } catch (Exception e) {
                    logger.warn("Automatic load available endpoint from " + this.currEndPoint + " failure.", e);
                }
            }, settings.getLoadAvailableListInterval(), settings.getLoadAvailableListInterval(), settings.getLoadAvailableListIntervalUnit());
        }
    }

    private synchronized void open(IGinXEndPoint endPoint) throws SessionException, ExecutionException {
        if (!isClosed) {
            return;
        }

        try {
            transport = new TSocket(new TConfiguration(), endPoint.getIp(), endPoint.getPort(), settings.getSocketTimeout(), settings.getConnectionTimeout());
            if (!transport.isOpen()) {
                transport.open();
            }
        } catch (TTransportException e) {
            throw new SessionException("Error occurs when open transport at server.", e);
        }

        client = new IService.Client(new TBinaryProtocol(transport));

        OpenSessionReq req = new OpenSessionReq();
        req.setUsername(settings.getUsername());
        req.setPassword(settings.getPassword());

        try {
            OpenSessionResp resp = client.openSession(req);
            RpcUtils.verifySuccess(resp.status);
            sessionId = resp.getSessionId();
        } catch (TException e) {
            throw new SessionException("Error occurs when open session at server.", e);
        } catch (ExecutionException e) {
            transport.close();
            throw e;
        }
        isClosed = false;

        client = newSynchronizedClient(client);
    }

    public synchronized void close() throws SessionException {
        if (this.isClosed) {
            return;
        }
        CloseSessionReq req = new CloseSessionReq(this.sessionId);
        try {
            client.closeSession(req);
        } catch (TException e) {
            throw new SessionException(
                    "Error occurs when closing session at server. Maybe server is shutdown.", e);
        } finally {
            this.isClosed = true;
            this.currEndPoint = null;
            if (transport != null) {
                transport.close();
            }
            if (this.autoLoadService != null) {
                this.autoLoadService.shutdownNow();
            }
        }
    }

    public int getReplicaNum() throws SessionException, ExecutionException {
        GetReplicaNumReq req = new GetReplicaNumReq(sessionId);
        GetReplicaNumResp resp;
        try {
            resp = client.getReplicaNum(req);
            RpcUtils.verifySuccess(resp.status);
        } catch (TException e) {
            boolean reconnect = reconnect();
            if (!reconnect && settings.isEnableHighAvailable()) {
                try {
                    open();
                    reconnect = true;
                } catch (Exception e1) {
                    logger.warn("switch to other iginx failure: ", e);
                }
            }
            if (reconnect) {
                try {
                    req.setSessionId(sessionId);
                    resp = client.getReplicaNum(req);
                    RpcUtils.verifySuccess(resp.status);
                } catch (TException e1) {
                    throw new SessionException(e1);
                }
            } else {
                throw new SessionException("Fail to reconnect to server " + currEndPoint + ". Please check server status");
            }
        }
        return resp.replicaNum;
    }

    private boolean reconnect() {
        boolean flag = false;
        for (int i = 0; i < settings.getMaxRetryTimes(); i++) {
            try {
                if (transport != null) {
                    close();
                    open(this.currEndPoint);
                    flag = true;
                    break;
                }
            } catch (Exception e) {
                try {
                    Thread.sleep(RETRY_INTERVAL_MS);
                } catch (InterruptedException e1) {
                    logger.error("reconnect is interrupted.", e1);
                    Thread.currentThread().interrupt();
                }
            }
        }
        return flag;
    }

    static IService.Iface newSynchronizedClient(IService.Iface client) {
        return (IService.Iface) Proxy.newProxyInstance(IGinXSession.class.getClassLoader(), new Class[]{IService.Iface.class}, new SynchronizedHandler(client));
    }

}
