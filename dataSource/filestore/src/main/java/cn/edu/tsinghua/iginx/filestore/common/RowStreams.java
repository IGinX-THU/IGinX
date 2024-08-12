package cn.edu.tsinghua.iginx.filestore.common;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream.EmptyRowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.read.FilterRowStreamWrapper;
import cn.edu.tsinghua.iginx.engine.shared.data.read.MergeFieldRowStreamWrapper;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;

import java.util.List;

public class RowStreams {

  private RowStreams() {
  }

  private static final EmptyRowStream EMPTY = new EmptyRowStream();

  public static RowStream empty() {
    return EMPTY;
  }

  public static RowStream filter(RowStream rowStream, Filter filter) {
    return new FilterRowStreamWrapper(rowStream, filter);
  }

  public static RowStream merge(List<RowStream> rowStreams) throws PhysicalException {
    if (rowStreams.isEmpty()) {
      return empty();
    } else if (rowStreams.size() == 1) {
      return rowStreams.get(0);
    } else {
      return new MergeFieldRowStreamWrapper(rowStreams);
    }
  }


}
