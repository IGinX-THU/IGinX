package cn.edu.tsinghua.iginx.parquet.manager.dummy;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Column;
import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.parquet.manager.Manager;
import java.util.Collections;
import java.util.List;

public class EmptyManager implements Manager {
  @Override
  public RowStream project(List<String> paths, TagFilter tagFilter, Filter filter)
      throws PhysicalException {
    return new RowStream() {

      private final Header header = new Header(Field.KEY, Collections.emptyList());

      @Override
      public Header getHeader() {
        return header;
      }

      @Override
      public void close() {}

      @Override
      public boolean hasNext() {
        return false;
      }

      @Override
      public Row next() {
        return null;
      }
    };
  }

  @Override
  public void insert(DataView dataView) throws PhysicalException {
    throw new PhysicalException("insert is not supported");
  }

  @Override
  public void delete(List<String> paths, List<KeyRange> keyRanges, TagFilter tagFilter)
      throws PhysicalException {
    throw new PhysicalException("delete is not supported");
  }

  @Override
  public List<Column> getColumns() throws PhysicalException {
    return Collections.emptyList();
  }

  @Override
  public KeyInterval getKeyInterval() throws PhysicalException {
    return new KeyInterval(0, 0);
  }

  @Override
  public void close() throws Exception {}
}
