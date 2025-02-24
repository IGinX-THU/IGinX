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

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class QueryResourceManager {

  private final ConcurrentMap<Long, RequestContext> queries;

  private final ConcurrentMap<Long, Instant> lastAccessTimeMap;

  private static final ResourceCleaner cleaner = new ResourceCleaner();

  private static final int cleanupInterval = 60; // minute

  private QueryResourceManager() {
    this.queries = new ConcurrentHashMap<>();
    this.lastAccessTimeMap = new ConcurrentHashMap<>();
    cleaner.startWithInterval(cleanupInterval);
  }

  public static QueryResourceManager getInstance() {
    return QueryManagerHolder.INSTANCE;
  }

  public void registerQuery(long queryId, RequestContext context) {
    queries.put(queryId, context);
    lastAccessTimeMap.put(queryId, Instant.now());
  }

  public RequestContext getQuery(long queryId) {
    lastAccessTimeMap.put(queryId, Instant.now());
    return queries.get(queryId);
  }

  public Set<Long> getQueryIds() {
    return queries.keySet();
  }

  public Instant getLastAccessTime(long queryId) {
    return lastAccessTimeMap.get(queryId);
  }

  public void releaseQuery(long queryId) throws PhysicalException {
    if (!queries.containsKey(queryId)) {
      return;
    }
    getQuery(queryId).getResult().cleanup();
    getQuery(queryId).closeResources();
    queries.remove(queryId);
    lastAccessTimeMap.remove(queryId);
  }

  private static class QueryManagerHolder {

    private static final QueryResourceManager INSTANCE = new QueryResourceManager();
  }
}
