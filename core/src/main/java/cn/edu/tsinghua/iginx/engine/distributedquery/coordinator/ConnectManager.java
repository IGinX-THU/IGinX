/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.engine.distributedquery.coordinator;

import cn.edu.tsinghua.iginx.engine.shared.source.IGinXSource;
import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.session.Session;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConnectManager.class);

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
      LOGGER.error("open session failed, because: ", e);
      return null;
    }
    connnectMap.put(source, session);
    return session;
  }
}
