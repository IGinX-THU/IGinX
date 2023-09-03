package cn.edu.tsinghua.iginx.mongodb.local;

import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.mongodb.local.entity.PathTree;
import cn.edu.tsinghua.iginx.mongodb.local.query.ClientQuery;
import com.mongodb.client.MongoClient;
import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;

public class LocalStorage {
  private final MongoClient client;

  private final Logger logger;

  public LocalStorage(MongoClient client, Logger logger) {
    this.client = client;
    this.logger = logger;
  }

  public RowStream query(List<String> patterns, Filter filter) {
    PathTree pathTree = new PathTree();
    for (String pattern : patterns) {
      pathTree.put(Arrays.stream(pattern.split("\\.")).iterator());
    }

    try {
      return new ClientQuery(this.client).query(pathTree, filter);
    } catch (Exception e) {
      logger.error("dummy project {} where {}", patterns, filter);
      logger.error("failed to dummy query ", e);
      throw e;
    }
  }
}
