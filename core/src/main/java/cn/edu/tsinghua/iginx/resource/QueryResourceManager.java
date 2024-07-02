/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
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

  public void releaseQuery(long queryId) {
    queries.remove(queryId);
  }

  private static class QueryManagerHolder {

    private static final QueryResourceManager INSTANCE = new QueryResourceManager();
  }
}
