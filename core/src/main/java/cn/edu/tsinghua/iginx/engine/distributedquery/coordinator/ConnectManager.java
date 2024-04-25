package cn.edu.tsinghua.iginx.engine.distributedquery.coordinator;

import cn.edu.tsinghua.iginx.engine.shared.source.IGinXSource;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.session.Session;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectManager {

  private static final Logger logger = LoggerFactory.getLogger(ConnectManager.class);

  private static class ConnectManagerHolder {
    private static final ConnectManager INSTANCE = new ConnectManager();
  }

  public static ConnectManager getInstance() {
    return ConnectManagerHolder.INSTANCE;
  }

  private final Map<IGinXSource, Session> connnectMap = new ConcurrentHashMap<>();

  private void addConnect(IGinXSource source, Session session) {
    connnectMap.put(source, session);
  }

  private void remove(IGinXSource source) {
    connnectMap.remove(source);
  }

  public Session getSession(IGinXSource source) {
    if (connnectMap.containsKey(source)) {
      return connnectMap.get(source);
    }

    Session session = new Session(source.getIp(), source.getPort());
    try {
      session.openSession();
    } catch (SessionException e) {
      logger.error("open session failed, because: ", e);
      return null;
    }
    connnectMap.put(source, session);
    return session;
  }
}
