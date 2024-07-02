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
