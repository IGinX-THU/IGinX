package cn.edu.tsinghua.iginx.filestore.struct.legacy.filesystem;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.filestore.struct.DataTarget;
import cn.edu.tsinghua.iginx.filestore.struct.FileManager;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.filesystem.exec.LocalExecutor;
import cn.edu.tsinghua.iginx.filestore.thrift.DataBoundary;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import cn.edu.tsinghua.iginx.utils.Pair;

import javax.annotation.Nullable;
import javax.annotation.WillCloseWhenClosed;
import java.io.IOException;
import java.util.List;

public class LegacyFilesystemWrapper implements FileManager {

  private final LocalExecutor executor;

  public LegacyFilesystemWrapper(@WillCloseWhenClosed LocalExecutor executor) {
    this.executor = executor;
  }

  @Override
  public DataBoundary getBoundary(@Nullable String prefix) throws IOException {
    try {
      Pair<ColumnsInterval, KeyInterval> boundary = executor.getBoundaryOfStorage(prefix);
      KeyInterval keyInterval = boundary.v;
      ColumnsInterval columnsInterval = boundary.k;
      DataBoundary dataBoundary = new DataBoundary(keyInterval.getStartKey(), keyInterval.getEndKey());
      dataBoundary.setStartColumn(columnsInterval.getStartColumn());
      dataBoundary.setEndColumn(columnsInterval.getEndColumn());
      return dataBoundary;
    } catch (PhysicalException e) {
      throw new IOException(e);
    }
  }

  @Override
  public RowStream query(DataTarget target, @Nullable AggregateType aggregate) throws IOException {
    if (aggregate != null) {
      throw new UnsupportedOperationException("LegacyFilesystem does not support aggregate");
    }

    Filter filter = target.getFilter();
    List<String> patterns = target.getPatterns();
    TagFilter tagFilter = target.getTagFilter(); // tagFilter is ignored

    TaskExecuteResult result = executor.executeDummyProjectTask(patterns, filter);
    if(result.getException()!=null){
      throw new IOException(result.getException());
    }
    return result.getRowStream();
  }

  @Override
  public void delete(DataTarget target) throws IOException {
    throw new UnsupportedOperationException("LegacyFilesystem does not support delete");
  }

  @Override
  public void insert(DataView data) throws IOException {
    throw new UnsupportedOperationException("LegacyFilesystem does not support insert");
  }

  @Override
  public void close() throws IOException {
    executor.close();
  }

}
