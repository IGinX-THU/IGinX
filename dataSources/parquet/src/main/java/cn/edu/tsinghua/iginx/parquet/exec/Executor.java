package cn.edu.tsinghua.iginx.parquet.exec;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Column;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.List;

public interface Executor {

  TaskExecuteResult executeProjectTask(
      List<String> paths,
      TagFilter tagFilter,
      Filter filter,
      String storageUnit,
      boolean isDummyStorageUnit);

  TaskExecuteResult executeInsertTask(DataView dataView, String storageUnit);

  TaskExecuteResult executeDeleteTask(
      List<String> paths, List<KeyRange> keyRanges, TagFilter tagFilter, String storageUnit);

  List<Column> getColumnsOfStorageUnit(String storageUnit) throws PhysicalException;

  Pair<ColumnsInterval, KeyInterval> getBoundaryOfStorage(String dataPrefix)
      throws PhysicalException;

  void close() throws PhysicalException;
}
