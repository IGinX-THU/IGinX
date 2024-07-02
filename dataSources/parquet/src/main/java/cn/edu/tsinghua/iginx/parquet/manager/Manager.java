package cn.edu.tsinghua.iginx.parquet.manager;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Column;
import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import java.util.List;

public interface Manager extends AutoCloseable {

  RowStream project(List<String> paths, TagFilter tagFilter, Filter filter)
      throws PhysicalException;

  void insert(DataView dataView) throws PhysicalException;

  void delete(List<String> paths, List<KeyRange> keyRanges, TagFilter tagFilter)
      throws PhysicalException;;

  List<Column> getColumns() throws PhysicalException;

  KeyInterval getKeyInterval() throws PhysicalException;
}
