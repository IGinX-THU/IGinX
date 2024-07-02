package cn.edu.tsinghua.iginx.session_v2.internal;

import cn.edu.tsinghua.iginx.session_v2.*;
import cn.edu.tsinghua.iginx.session_v2.exception.IginXException;
import cn.edu.tsinghua.iginx.thrift.CloseSessionReq;
import cn.edu.tsinghua.iginx.thrift.IService;
import cn.edu.tsinghua.iginx.thrift.OpenSessionReq;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IginXClientImpl implements IginXClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(IginXClientImpl.class);

  private final IService.Iface client;

  private final TTransport transport;

  private final Lock lock; // IService.Iface 的 lock，保证多个线程在使用 client 时候不会互相影响

  private final long sessionId;

  private boolean isClosed;

  private final MeasurementMapper measurementMapper;

  private final ResultMapper resultMapper;

  private final Collection<AutoCloseable> autoCloseables = new CopyOnWriteArrayList<>();

  public IginXClientImpl(IginXClientOptions options) {
    Arguments.checkNotNull(options, "IginXClientOptions");

    lock = new ReentrantLock();
    measurementMapper = new MeasurementMapper();
    resultMapper = new ResultMapper();

    try {
      transport = new TSocket(options.getHost(), options.getPort());
      transport.open();
      client = new IService.Client(new TBinaryProtocol(transport));
    } catch (TTransportException e) {
      throw new IginXException("Open socket error: ", e);
    }

    try {
      OpenSessionReq req = new OpenSessionReq();
      req.setUsername(options.getUsername());
      req.setPassword(options.getPassword());
      sessionId = client.openSession(req).getSessionId();

    } catch (TException e) {
      throw new IginXException("Open session error: ", e);
    }
  }

  @Override
  public synchronized WriteClient getWriteClient() {
    checkIsClosed();
    return new WriteClientImpl(this, measurementMapper);
  }

  @Override
  public synchronized AsyncWriteClient getAsyncWriteClient() {
    checkIsClosed();
    return new AsyncWriteClientImpl(this, measurementMapper, autoCloseables);
  }

  @Override
  public synchronized QueryClient getQueryClient() {
    checkIsClosed();
    return new QueryClientImpl(this, resultMapper);
  }

  @Override
  public synchronized DeleteClient getDeleteClient() {
    checkIsClosed();
    return new DeleteClientImpl(this, measurementMapper);
  }

  @Override
  public synchronized UsersClient getUserClient() {
    checkIsClosed();
    return new UsersClientImpl(this);
  }

  @Override
  public synchronized ClusterClient getClusterClient() {
    checkIsClosed();
    return new ClusterClientImpl(this);
  }

  @Override
  public TransformClient getTransformClient() {
    checkIsClosed();
    return new TransformClientImpl(this);
  }

  void checkIsClosed() {
    if (isClosed) {
      throw new IginXException("Session has been closed.");
    }
  }

  public synchronized boolean isClosed() {
    return isClosed;
  }

  IService.Iface getClient() {
    return client;
  }

  Lock getLock() {
    return lock;
  }

  long getSessionId() {
    return sessionId;
  }

  @Override
  public synchronized void close() {
    if (isClosed) {
      LOGGER.warn("Client has been closed.");
      return;
    }

    autoCloseables.stream()
        .filter(Objects::nonNull)
        .forEach(
            resource -> {
              try {
                resource.close();
              } catch (Exception e) {
                LOGGER.warn("Exception was thrown while closing: {}", resource, e);
              }
            });

    CloseSessionReq req = new CloseSessionReq(sessionId);
    try {
      client.closeSession(req);
    } catch (TException e) {
      throw new IginXException("Close session error: ", e);
    } finally {
      if (transport != null) {
        transport.close();
      }
      isClosed = true;
    }
  }
}
