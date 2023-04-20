package cn.edu.tsinghua.iginx.filesystem.exec;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalTaskExecuteFailureException;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Timeseries;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.TimeRange;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.BitmapView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.ColumnDataView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RowDataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.Delete;
import cn.edu.tsinghua.iginx.engine.shared.operator.Insert;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.filesystem.file.property.FileMeta;
import cn.edu.tsinghua.iginx.filesystem.filesystem.FileSystemImpl;
import cn.edu.tsinghua.iginx.filesystem.query.FSResultTable;
import cn.edu.tsinghua.iginx.filesystem.query.FileSystemHistoryQueryRowStream;
import cn.edu.tsinghua.iginx.filesystem.query.FileSystemQueryRowStream;
import cn.edu.tsinghua.iginx.filesystem.file.property.FilePath;
import cn.edu.tsinghua.iginx.filesystem.tools.FilterTransformer;
import cn.edu.tsinghua.iginx.filesystem.wrapper.Record;
import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesRange;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;

import static cn.edu.tsinghua.iginx.thrift.DataType.*;

public class LocalExecutor implements Executor {
    FileSystemImpl fileSystem = new FileSystemImpl();
    private static final Logger logger = LoggerFactory.getLogger(LocalExecutor.class);

    @Override
    public TaskExecuteResult executeProjectTask(Project project, byte[] filter, String storageUnit, boolean isDummyStorageUnit) {
        List<String> series = project.getPatterns();
        TagFilter tagFilter = project.getTagFilter();
        Filter filterEntity = FilterTransformer.toFilter(filter);
        if (isDummyStorageUnit) {
            return executeDummyProjectTask(series, filterEntity);
        }

        return executeQueryTask(storageUnit, series, tagFilter, filterEntity);
    }

    public TaskExecuteResult executeQueryTask(String storageUnit, List<String> series, TagFilter tagFilter, Filter filter) {
        try {
            List<javafx.util.Pair<FilePath,Integer>> pathMap = new ArrayList<>();
            List<FSResultTable> result = new ArrayList<>();
            // fix it 如果有远程文件系统则需要server
            FileSystemImpl fileSystem = new FileSystemImpl();
            logger.info("[Query] execute query file: " + series);
            for (String path : series) {
                result.addAll(fileSystem.readFile(new File(FilePath.toIginxPath(storageUnit,path)), tagFilter, filter));
                FilePath filePath = new FilePath(storageUnit, path);
                pathMap.add(new javafx.util.Pair<>(filePath,result.size()));
            }
            RowStream rowStream = new FileSystemQueryRowStream(result,pathMap,storageUnit);
            return new TaskExecuteResult(rowStream);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            return new TaskExecuteResult(new PhysicalTaskExecuteFailureException("execute project task in fileSystem failure", e));
        }
    }

    private TaskExecuteResult executeDummyProjectTask(List<String> series, Filter filter) {
        try {
            List<javafx.util.Pair<FilePath,Integer>> pathMap = new ArrayList<>();
            List<FSResultTable> result = new ArrayList<>();
            // fix it 如果有远程文件系统则需要server
            FileSystemImpl fileSystem = new FileSystemImpl();
            logger.info("[Query] execute query file: " + series);
            for (String path : series) {
                result.addAll(fileSystem.readFile(new File(FilePath.toNormalFilePath(path)), filter));
                FilePath filePath = new FilePath(null, path);
                pathMap.add(new javafx.util.Pair<>(filePath,result.size()));
            }
            RowStream rowStream = new FileSystemHistoryQueryRowStream(result,pathMap);
            return new TaskExecuteResult(rowStream);
        } catch (Exception e) {
            logger.error(e.getMessage());
            return new TaskExecuteResult(new PhysicalTaskExecuteFailureException("execute project task in fileSystem failure", e));
        }
    }

    @Override
    public TaskExecuteResult executeInsertTask(Insert insert, String storageUnit) {
        DataView dataView = insert.getData();
        Exception e = null;
        switch (dataView.getRawDataType()) {
            case Row:
            case NonAlignedRow:
                e = insertRowRecords((RowDataView) dataView, storageUnit);
                break;
            case Column:
            case NonAlignedColumn:
                e = insertColumnRecords((ColumnDataView) dataView, storageUnit);
                break;
        }
        if (e != null) {
            e.printStackTrace();
            return new TaskExecuteResult(null, new PhysicalException("execute insert task in fileSystem failure", e));
        }
        return new TaskExecuteResult(null, null);
    }

    private Exception insertRowRecords(RowDataView data, String storageUnit) {
        List<List<Record>> valList = new ArrayList<>();
        List<File> fileList = new ArrayList<>();
        List<Boolean> ifAppend = new ArrayList<>();
        List<Map<String, String>> tagList = new ArrayList<>();

        if (fileSystem == null) {
            return new PhysicalTaskExecuteFailureException("get fileSystem failure!");
        }

        for (int j = 0; j < data.getPathNum(); j++) {
            fileList.add(new File(FilePath.toIginxPath(storageUnit,data.getPath(j))));
            tagList.add(data.getTags(j));
            ifAppend.add(false);// always append, fix it!
        }

        for (int j = 0; j < data.getPathNum(); j++) {
            valList.add(new ArrayList<>());
        }

        for (int i = 0; i < data.getTimeSize(); i++) {
            List<Record> val = new ArrayList<>();
            BitmapView bitmapView = data.getBitmapView(i);
            int index = 0;
            for (int j = 0; j < data.getPathNum(); j++) {
                if (bitmapView.get(j)) {
                    DataType dataType = null;
                    switch (data.getDataType(j)) {
                        case BOOLEAN:
                            dataType=BOOLEAN;
                            break;
                        case INTEGER:
                            dataType=INTEGER;
                            break;
                        case LONG:
                            dataType=LONG;
                            break;
                        case FLOAT:
                            dataType=FLOAT;
                            break;
                        case DOUBLE:
                            dataType=DOUBLE;
                            break;
                        case BINARY:
                            dataType=BINARY;
                            break;
                    }
                    valList.get(j).add(new Record(data.getKey(i), dataType, data.getValue(i, index)));
                    index++;
                }
            }
        }
        try {
            logger.info("开始数据写入");
            fileSystem.writeFiles(fileList, valList, tagList, ifAppend);
        } catch (Exception e) {
            logger.error("encounter error when write points to fileSystem: ", e);
        } finally {
            logger.info("数据写入完毕！");
        }
        return null;
    }

    private Exception insertColumnRecords(ColumnDataView data, String storageUnit) {
        List<List<Record>> valList = new ArrayList<>();
        List<File> fileList = new ArrayList<>();
        List<Boolean> ifAppend = new ArrayList<>();
        List<Map<String, String>> tagList = new ArrayList<>();

        if (fileSystem == null) {
            return new PhysicalTaskExecuteFailureException("get fileSystem failure!");
        }

        for (int j = 0; j < data.getPathNum(); j++) {
            fileList.add(new File(FilePath.toIginxPath(storageUnit,data.getPath(j))));
            tagList.add(data.getTags(j));
            ifAppend.add(false);// always append, fix it!
        }

        for (int i = 0; i < data.getPathNum(); i++) {
            List<Record> val = new ArrayList<>();
            BitmapView bitmapView = data.getBitmapView(i);
            int index = 0;
            for (int j = 0; j < data.getTimeSize(); j++) {
                if (bitmapView.get(j)) {
                    switch (data.getDataType(i)) {
                        case BOOLEAN:
                            val.add(new Record(data.getKey(j), BOOLEAN, data.getValue(i, index)));
                            break;
                        case INTEGER:
                            val.add(new Record(data.getKey(j), INTEGER, data.getValue(i, index)));
                            break;
                        case LONG:
                            val.add(new Record(data.getKey(j), LONG, data.getValue(i, index)));
                            break;
                        case FLOAT:
                            val.add(new Record(data.getKey(j), FLOAT, data.getValue(i, index)));
                            break;
                        case DOUBLE:
                            val.add(new Record(data.getKey(j), DOUBLE, data.getValue(i, index)));
                            break;
                        case BINARY:
                            val.add(new Record(data.getKey(j), BINARY, data.getValue(i, index)));
                            break;
                    }
                    index++;
                }
            }
            valList.add(val);
        }

        try {
            logger.info("开始数据写入");
            fileSystem.writeFiles(fileList, valList, tagList,ifAppend);
        } catch (Exception e) {
            logger.error("encounter error when write points to fileSystem: ", e);
        } finally {
            logger.info("数据写入完毕！");
        }
        return null;
    }

    @Override
    public TaskExecuteResult executeDeleteTask(Delete delete, String storageUnit) {
        if(storageUnit.equals("unit0000000001")) {
            System.out.println("iiiii");
        }
        List<String> paths = delete.getPatterns();
        if (delete.getTimeRanges() == null || delete.getTimeRanges().size() == 0) { // 没有传任何 time range
            List<File> fileList = new ArrayList<>();
            if (paths.size() == 1 && paths.get(0).equals("*") && delete.getTagFilter() == null) {
                try {
                    fileSystem.deleteFile(new File(FilePath.toIginxPath(storageUnit,null)));
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.error("encounter error when clear data: " + e.getMessage());
                }
            } else {
                for (String path : paths) {
                    fileList.add(new File(FilePath.toIginxPath(storageUnit,path)));
                }
                try {
                    fileSystem.deleteFiles(fileList,delete.getTagFilter());
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.error("encounter error when clear data: " + e.getMessage());
                }
            }
        } else {
            List<File> fileList = new ArrayList<>();
            try {
                if (paths.size() != 0) {
                    for (String path : paths) {
                        fileList.add(new File(FilePath.toIginxPath(storageUnit,path)));
                    }
                    for (TimeRange timeRange: delete.getTimeRanges()) {
                        fileSystem.trimFilesContent(fileList,delete.getTagFilter() ,timeRange.getActualBeginTime(), timeRange.getActualEndTime());
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
                logger.error("encounter error when delete data: " + e.getMessage());
                throw new RuntimeException(e);
            }
        }
        return new TaskExecuteResult(null, null);
    }

    @Override
    public List<Timeseries> getTimeSeriesOfStorageUnit(String storageUnit) throws PhysicalException {
        List<Timeseries> files = new ArrayList<>();

        File directory = new File(FilePath.toIginxPath(storageUnit,null));// fix it , 这里的 storageUnit 需要转化为一个目录

        List<javafx.util.Pair<File, FileMeta>> res = fileSystem.getAllIginXFiles(directory);

        for (javafx.util.Pair<File, FileMeta> pair : res) {
            File file = pair.getKey();
            FileMeta meta = pair.getValue();
            files.add(new Timeseries(FilePath.convertAbsolutePathToSeries(file.getAbsolutePath(),file.getName(), storageUnit),meta.getDataType(),meta.getTag()));
        }
        return files;
    }

    @Override
    public Pair<TimeSeriesRange, TimeInterval> getBoundaryOfStorage(String prefix) throws PhysicalException {
        File directory = new File(FilePath.toNormalFilePath(prefix));

        List<File> files = fileSystem.getBoundaryFiles(directory);

        File minPathFile = files.get(0);
        File maxPathFile = files.get(1);

        TimeSeriesRange tsInterval = null;
        if (prefix == null)
            tsInterval = new TimeSeriesInterval(FilePath.convertAbsolutePathToSeries(minPathFile.getAbsolutePath(),minPathFile.getName(), null),
                FilePath.convertAbsolutePathToSeries(maxPathFile.getAbsolutePath(),maxPathFile.getName(), null));
        else
            tsInterval = new TimeSeriesInterval(prefix, StringUtils.nextString(prefix));

        //对于pb级的文件系统，遍历是不可能的，直接接入
        List<Long> time = fileSystem.getBoundaryTime(directory);
        TimeInterval timeInterval = new TimeInterval(time.get(0)==Long.MAX_VALUE?0:time.get(0),
            time.get(1)==Long.MIN_VALUE?Long.MAX_VALUE:time.get(0));

        return new Pair<>(tsInterval, timeInterval);
    }

    @Override
    public void close() throws PhysicalException {

    }
}
