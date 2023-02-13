package cn.edu.tsinghua.iginx.engine.physical.fault;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.physical.storage.IStorage;
import cn.edu.tsinghua.iginx.engine.physical.storage.IStorageManager;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Timeseries;
import cn.edu.tsinghua.iginx.engine.physical.storage.fault_tolerance.Connector;
import cn.edu.tsinghua.iginx.engine.physical.task.StoragePhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import cn.edu.tsinghua.iginx.engine.shared.source.SourceType;
import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesRange;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

public class FaultToleranceStorageTaskRepeaterTest {

    @Test
    public void testRepeater() throws PhysicalException {
        Map<Long, IStorage> storageMap = new HashMap<>();
        storageMap.put(1L, new AbstractTestStorage() {
            @Override
            public TaskExecuteResult execute(StoragePhysicalTask task) {
                assertFalse(task.isSkipData());
                assertEquals(0, task.getLastTimestamp());
                return new TaskExecuteResult(new RowStream() {

                    private final Header header = new Header(Field.KEY, Arrays.asList(
                            new Field("m.a", DataType.LONG),
                            new Field("m.d", DataType.LONG),
                            new Field("m.e", DataType.LONG)
                    ));

                    private final long startKey = 100;

                    private final long exceptionKey = 200;

                    private long index = startKey;

                    @Override
                    public Header getHeader() throws PhysicalException {
                        return header;
                    }

                    @Override
                    public void close() throws PhysicalException {

                    }

                    @Override
                    public boolean hasNext() throws PhysicalException {
                        if (index > exceptionKey) {
                            throw new PhysicalException("unexpected crush for storage 1");
                        }
                        return true;
                    }

                    @Override
                    public Row next() throws PhysicalException {
                        Row row = new Row(header, index, new Object[] { index, index + 3, index + 4 });
                        index++;
                        return row;
                    }
                });
            }
        });
        storageMap.put(2L, new AbstractTestStorage() {
            @Override
            public TaskExecuteResult execute(StoragePhysicalTask task) {
                assertTrue(task.isSkipData());
                assertEquals(200, task.getLastTimestamp());
                return new TaskExecuteResult(new RowStream() {

                    private final Header header = new Header(Field.KEY, Arrays.asList(
                            new Field("m.b", DataType.LONG),
                            new Field("m.d", DataType.LONG),
                            new Field("m.e", DataType.LONG)
                    ));

                    private final long startKey = 201;

                    private final long exceptionKey = 400;

                    private long index = startKey;

                    @Override
                    public Header getHeader() throws PhysicalException {
                        return header;
                    }

                    @Override
                    public void close() throws PhysicalException {

                    }

                    @Override
                    public boolean hasNext() throws PhysicalException {
                        if (index > exceptionKey) {
                            throw new PhysicalException("unexpected crush for storage 2");
                        }
                        return true;
                    }

                    @Override
                    public Row next() throws PhysicalException {
                        Row row = new Row(header, index, new Object[] { index + 1, index + 3, index + 4 });
                        index++;
                        return row;
                    }
                });
            }
        });
        storageMap.put(3L, new AbstractTestStorage() {
            @Override
            public TaskExecuteResult execute(StoragePhysicalTask task) {
                assertTrue(task.isSkipData());
                assertEquals(400, task.getLastTimestamp());
                return new TaskExecuteResult(new RowStream() {

                    private final Header header = new Header(Field.KEY, Arrays.asList(
                            new Field("m.c", DataType.LONG),
                            new Field("m.d", DataType.LONG),
                            new Field("m.e", DataType.LONG)
                    ));

                    private final long startKey = 401;

                    private final long exceptionKey = 600;

                    private long index = startKey;

                    @Override
                    public Header getHeader() throws PhysicalException {
                        return header;
                    }

                    @Override
                    public void close() throws PhysicalException {

                    }

                    @Override
                    public boolean hasNext() throws PhysicalException {
                        return index <= exceptionKey;
                    }

                    @Override
                    public Row next() throws PhysicalException {
                        Row row = new Row(header, index, new Object[] { index + 2, index + 3, index + 4 });
                        index++;
                        return row;
                    }
                });
            }
        });
        IStorageManager storageManager = new TestStorageManager(storageMap);
        RequestContext context = new RequestContext();
        context.setEnableFaultTolerance(true);
        StoragePhysicalTask task = new StoragePhysicalTask(Arrays.asList(new Project(EmptySource.EMPTY_SOURCE, Collections.singletonList("a.a.*"), null)), context, true);
        task.setBackup(new long[] {2, 3}, new String[] {"du2", "du3"});
        TaskExecuteResult result = storageManager.getIStorage(1L).execute(task);
        result = new FaultToleranceStorageTaskRepeater(task, result, storageManager).getFinalResult();
        Table table = (Table) result.getRowStream();
        System.out.println(table);
    }

    abstract static class AbstractTestStorage implements IStorage {

        @Override
        public Connector getConnector() {
            return null;
        }

        @Override
        public List<Timeseries> getTimeSeries() throws PhysicalException {
            return null;
        }

        @Override
        public Pair<TimeSeriesRange, TimeInterval> getBoundaryOfStorage(String prefix) throws PhysicalException {
            return null;
        }

        @Override
        public void release() throws PhysicalException {

        }
    }

    static class TestStorageManager implements IStorageManager {

        private final Map<Long, IStorage> storageMap;

        public TestStorageManager(Map<Long, IStorage> storageMap) {
            this.storageMap = storageMap;
        }

        @Override
        public IStorage getIStorage(long id) {
            return storageMap.get(id);
        }
    }

    static class EmptySource implements Source {

        public static final EmptySource EMPTY_SOURCE = new EmptySource();

        @Override
        public SourceType getType() {
            return null;
        }

        @Override
        public Source copy() {
            return null;
        }
    }

}
