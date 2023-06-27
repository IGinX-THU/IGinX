package cn.edu.tsinghua.iginx.filesystem.exec;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Column;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Timeseries;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.Delete;
import cn.edu.tsinghua.iginx.engine.shared.operator.Insert;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsRange;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesRange;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.List;

public interface Executor {

    TaskExecuteResult executeProjectTask(
            List<String> paths,
            TagFilter tagFilter,
            byte[] filter,
            String storageUnit,
            boolean isDummyStorageUnit);

    TaskExecuteResult executeInsertTask(DataView dataView, String storageUnit);

    TaskExecuteResult executeDeleteTask(List<String> paths, List<KeyRange> keyRanges, TagFilter tagFilter, String storageUnit);

    List<Column> getColumnsRangesOfStorageUnit(String storageUnit) throws PhysicalException;

    Pair<ColumnsRange, KeyInterval> getBoundaryOfStorage(String prefix)
            throws PhysicalException;

    void close() throws PhysicalException;
}
