package cn.edu.tsinghua.iginx.filestore.struct.units;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.MergeFieldRowStreamWrapper;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.filestore.struct.DataTarget;
import cn.edu.tsinghua.iginx.filestore.struct.FileManager;
import cn.edu.tsinghua.iginx.filestore.thrift.DataBoundary;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import org.apache.arrow.util.AutoCloseables;

import javax.annotation.Nullable;
import javax.annotation.WillNotClose;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class UnitsMerger implements FileManager {

  private final List<FileManager> managers;

  public UnitsMerger(@WillNotClose List<FileManager> managers) {
    this.managers = Objects.requireNonNull(managers);
  }

  @Override
  public DataBoundary getBoundary(@Nullable String prefix) throws IOException {
    long startKey = Long.MAX_VALUE;
    long endKey = Long.MIN_VALUE;
    String startColumn = "";
    String endColumn = "";
    boolean set = false;
    for (FileManager manager : managers) {
      DataBoundary boundary = manager.getBoundary(prefix);
      if (Objects.equals(boundary, new DataBoundary())) {
        continue;
      }
      startKey = Math.min(startKey, boundary.getStartKey());
      endKey = Math.max(endKey, boundary.getEndKey());
      if (!set) {
        startColumn = boundary.getStartColumn();
        endColumn = boundary.getEndColumn();
      }
      if (startColumn != null) {
        if (boundary.getStartColumn() == null) {
          startColumn = null;
        } else if (boundary.getStartColumn().compareTo(startColumn) < 0) {
          startColumn = boundary.getStartColumn();
        }
      }
      if (endColumn != null) {
        if (boundary.getEndColumn() == null) {
          endColumn = null;
        } else if (boundary.getEndColumn().compareTo(endColumn) > 0) {
          endColumn = boundary.getEndColumn();
        }
      }
      set = true;
    }
    if (startKey >= endKey) {
      return new DataBoundary();
    }
    DataBoundary boundary = new DataBoundary(startKey, endKey);
    boundary.setStartColumn(startColumn);
    boundary.setEndColumn(endColumn);
    return boundary;
  }

  @Override
  public RowStream query(DataTarget target, @Nullable AggregateType aggregate) throws IOException {
    if (aggregate != null) {
      throw new UnsupportedOperationException("aggregate is not supported in UnitsMerger");
    }
    List<RowStream> streams = new ArrayList<>();
    try {
      for (FileManager manager : managers) {
        streams.add(manager.query(target, null));
      }
      return new MergeFieldRowStreamWrapper(streams);
    } catch (IOException | PhysicalException e) {
      IOException ex = new IOException("Failed to query data from UnitsMerger", e);
      AutoCloseables.close(ex, streams);
      throw ex;
    }
  }

  @Override
  public void delete(DataTarget target) throws IOException {
    throw new UnsupportedOperationException("delete is not supported in UnitsMerger");
  }

  @Override
  public void insert(DataView data) throws IOException {
    throw new UnsupportedOperationException("insert is not supported in UnitsMerger");
  }

  @Override
  public void close() throws IOException {
  }
}
