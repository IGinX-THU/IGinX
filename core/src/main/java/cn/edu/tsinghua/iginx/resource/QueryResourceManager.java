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
package cn.edu.tsinghua.iginx.resource;

import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class QueryResourceManager {

  private final ConcurrentMap<Long, RequestContext> queries;

  private QueryResourceManager() {
    this.queries = new ConcurrentHashMap<>();
  }

  public static QueryResourceManager getInstance() {
    return QueryManagerHolder.INSTANCE;
  }

  public void registerQuery(long queryId, RequestContext context) {
    queries.put(queryId, context);
  }

  public RequestContext getQuery(long queryId) {
    return queries.get(queryId);
  }

  public boolean ownsQuery(long sessionId, long queryId) {
    RequestContext query = queries.get(queryId);
    return query != null && query.getSessionId() == sessionId;
  }

  public void releaseQuery(long queryId) {
    RequestContext context = queries.remove(queryId);
    close(context);
  }

  public void releaseSession(long sessionId) {
    queries.forEach(
        (id, context) -> {
          if (context.getSessionId() == sessionId && queries.remove(id, context)) close(context);
        });
  }

  private void close(RequestContext context) {
    if (context != null
        && context.getResult() != null
        && context.getResult().getResultStream() != null) {
      try {
        context.getResult().getResultStream().close();
      } catch (cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException e) {
        org.slf4j.LoggerFactory.getLogger(QueryResourceManager.class)
            .warn("Could not close query result", e);
      }
    }
  }

  private static class QueryManagerHolder {

    private static final QueryResourceManager INSTANCE = new QueryResourceManager();
  }
}
