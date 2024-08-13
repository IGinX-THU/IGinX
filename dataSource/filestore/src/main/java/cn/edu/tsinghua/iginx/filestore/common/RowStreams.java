package cn.edu.tsinghua.iginx.filestore.common;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream.EmptyRowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.read.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;

import javax.annotation.Nullable;
import java.util.List;

public class RowStreams {

  private RowStreams() {
  }

  private static final EmptyRowStream EMPTY = new EmptyRowStream();

  public static RowStream empty() {
    return EMPTY;
  }

  public static RowStream empty(Header header) {
    return new EmptyRowStream(header);
  }

  public static RowStream merged(List<RowStream> rowStreams) throws PhysicalException {
    if (rowStreams.isEmpty()) {
      return empty();
    } else if (rowStreams.size() == 1) {
      return rowStreams.get(0);
    } else {
      return new MergeFieldRowStreamWrapper(rowStreams);
    }
  }

  public static RowStream filtered(RowStream rowStream, @Nullable Filter filter) {
    if (Filters.isTrue(filter)) {
      return rowStream;
    }
    return new FilterRowStreamWrapper(rowStream, filter);
  }
}
