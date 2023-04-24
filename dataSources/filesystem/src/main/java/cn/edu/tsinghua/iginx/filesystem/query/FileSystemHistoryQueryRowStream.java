package cn.edu.tsinghua.iginx.filesystem.query;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.filesystem.file.property.FilePath;
import cn.edu.tsinghua.iginx.filesystem.wrapper.Record;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class FileSystemHistoryQueryRowStream implements RowStream {
    private final Header header;
    private final List<FSResultTable> rowData;
    private final int[][] indices;
    private final int[] round;
    private int hasMoreRecords = 0;

    public FileSystemHistoryQueryRowStream() {
        Field time = Field.KEY;
        List<Field> fields = new ArrayList<>();

        this.rowData = new ArrayList<>();
        ;
        this.indices = new int[0][1024 * 2];
        this.round = new int[0];
        this.header = new Header(time, fields);
        for (int i = 0; i < this.rowData.size(); i++) {
            if (this.rowData.get(i).getVal().size() != 0) hasMoreRecords++;
        }
    }

    // may fix it ，可能可以不用传pathMap
    public FileSystemHistoryQueryRowStream(List<FSResultTable> result, String root) {
        Field time = Field.KEY;
        List<Field> fields = new ArrayList<>();

        this.rowData = result;

        String series;
        for (FSResultTable resultTable : rowData) {
            File file = resultTable.getFile();
            series =
                    FilePath.convertAbsolutePathToSeries(
                            root, file.getAbsolutePath(), file.getName(), null);
            Field field = new Field(series, resultTable.getDataType(), resultTable.getTags());
            fields.add(field);
        }

        this.indices = new int[this.rowData.size()][1024 * 2];
        this.round = new int[this.rowData.size()];
        this.header = new Header(time, fields);
        for (int i = 0; i < this.rowData.size(); i++) {
            if (this.rowData.get(i).getVal().size() != 0) hasMoreRecords++;
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
            int index = round[i];
            List<Record> records = this.rowData.get(i).getVal();
            if (index == records.size()) { // 数据已经消费完毕了
                continue;
            }
            timestamp = Math.min(indices[i][index], timestamp);
        }
        if (timestamp == Long.MAX_VALUE) {
            return null;
        }
        Object[] values = new Object[rowData.size()];
        for (int i = 0; i < this.rowData.size(); i++) {
            int index = round[i];
            List<Record> records = this.rowData.get(i).getVal();
            if (index == records.size()) { // 数据已经消费完毕了
                continue;
            }
            byte[] val = (byte[]) records.get(index).getRawData();
            if (indices[i][index] == timestamp) {
                byte[] newVal = new byte[1];
                newVal[0] = val[indices[i][index]];
                Object value = newVal;
                values[i] = value;
                indices[i][index]++;
                if (indices[i][index] == val.length) {
                    round[i]++;
                    if (round[i] == records.size()) hasMoreRecords--;
                }
            }
        }
        return new Row(header, timestamp, values);
    }
}
