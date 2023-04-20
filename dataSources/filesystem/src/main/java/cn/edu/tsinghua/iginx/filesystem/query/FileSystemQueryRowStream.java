package cn.edu.tsinghua.iginx.filesystem.query;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.filesystem.file.property.FilePath;
import cn.edu.tsinghua.iginx.filesystem.wrapper.Record;
import cn.edu.tsinghua.iginx.thrift.DataType;
import javafx.util.Pair;
import jdk.nashorn.internal.runtime.ListAdapter;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class FileSystemQueryRowStream implements RowStream {
    private final Header header;
    private final List<FSResultTable> rowData;
    private final int[] indices;
    private int hasMoreRecords = 0;

    // may fix it ，可能可以不用传pathMap
    public FileSystemQueryRowStream(List<FSResultTable> result, List<Pair<FilePath,Integer>> pathMap,String storageUnit) {
        Field time = Field.KEY;
        List<Field> fields = new ArrayList<>();

        this.rowData = result;

//        int index = 0,pathIndex = 0;
//        int boundary = pathMap.get(pathIndex).getValue();
        String series;
        for (FSResultTable resultTable : rowData) {
//            if(index==boundary){
//                pathIndex++;
//                boundary = pathMap.get(pathIndex).getValue();
//                series = pathMap.get(pathIndex).getKey().getOriSeries();
//            }
//            index++;
            File file = resultTable.getFile();
            series=FilePath.convertAbsolutePathToSeries(file.getAbsolutePath(),file.getName(),storageUnit);
            Field field = new Field(series, resultTable.getDataType(), resultTable.getTags());// fix it 先假设查询的全是NormalFile类型
            fields.add(field);
        }

        this.indices = new int[this.rowData.size()];
        this.header = new Header(time, fields);
        for (int i = 0; i < this.rowData.size(); i++) {
            if(this.rowData.get(i).getVal().size()!=0)
                hasMoreRecords++;
        }
    }

    @Override
    public Header getHeader() throws PhysicalException {
        return header;
    }

    @Override
    public void close() throws PhysicalException {
        // need to do nothing
    }

    @Override
    public boolean hasNext() throws PhysicalException {
        return this.hasMoreRecords != 0;
    }

    @Override
    public Row next() throws PhysicalException {
        long timestamp = Long.MAX_VALUE;
        for (int i = 0; i < this.rowData.size(); i++) {
            int index = indices[i];
            List<Record> records = this.rowData.get(i).getVal();
            if (index == records.size()) { // 数据已经消费完毕了
                continue;
            }
            Record record = records.get(index);
            timestamp = Math.min(record.getKey(), timestamp);
        }
        if (timestamp == Long.MAX_VALUE) {
            return null;
        }
        Object[] values = new Object[rowData.size()];
        for (int i = 0; i < this.rowData.size(); i++) {
            int index = indices[i];
            List<Record> records = this.rowData.get(i).getVal();
            if (index == records.size()) { // 数据已经消费完毕了
                continue;
            }
            Record record = records.get(index);
            if (record.getKey() == timestamp) { // 考虑时间 ns may fix it
                DataType dataType = header.getField(i).getType();
                Object value = record.getRawData();
//                if (dataType == DataType.BINARY) {
//                    value = (String) value;
//                }
                values[i] = value;
                indices[i]++;
                if (indices[i] == records.size()) {
                    hasMoreRecords--;
                }
            }
        }
        return new Row(header, timestamp, values);
    }
}
