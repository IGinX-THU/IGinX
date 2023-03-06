package cn.edu.tsinghua.iginx.engine.shared.function.udf.utils;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import java.util.List;
import java.util.stream.Collectors;

public class RowUtils {

    public static Row constructNewRowWithKey(Header header, long key, List<Object> values) {
        Object[] rowValues = new Object[values.size()];
        for (int i = 0; i < values.size(); i++) {
            Object val = values.get(i);
            if (val instanceof String) {
                rowValues[i] = ((String) val).getBytes();
            } else {
                rowValues[i] = val;
            }
        }
        return new Row(header, key, rowValues);
    }

    public static Row constructNewRow(Header header, List<Object> values) {
        return constructNewRowWithKey(header, Row.NON_EXISTED_KEY, values);
    }

    public static Table constructNewTable(Header header, List<List<Object>> values) {
        List<Row> rowList = values.stream()
            .map(row -> constructNewRow(header, row))
            .collect(Collectors.toList());
        return new Table(header, rowList);
    }
}
