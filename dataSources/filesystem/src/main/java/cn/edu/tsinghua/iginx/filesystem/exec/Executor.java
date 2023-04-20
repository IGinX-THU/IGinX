package cn.edu.tsinghua.iginx.filesystem.exec;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Timeseries;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.TimeRange;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.Delete;
import cn.edu.tsinghua.iginx.engine.shared.operator.Insert;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesRange;
import cn.edu.tsinghua.iginx.utils.Pair;

import java.io.IOException;
import java.util.List;

public interface Executor {

    TaskExecuteResult executeProjectTask(Project project, byte[] filter,
                                         String storageUnit, boolean isDummyStorageUnit);

    TaskExecuteResult executeInsertTask(Insert insert, String storageUnit);

    TaskExecuteResult executeDeleteTask(Delete delete, String storageUnit);

    List<Timeseries> getTimeSeriesOfStorageUnit(String storageUnit) throws PhysicalException;

    Pair<TimeSeriesRange, TimeInterval> getBoundaryOfStorage(String prefix) throws PhysicalException;

    void close() throws PhysicalException;

}
