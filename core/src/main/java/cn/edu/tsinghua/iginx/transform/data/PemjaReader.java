package cn.edu.tsinghua.iginx.transform.data;

import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.transform.api.Reader;
import java.util.ArrayList;
import java.util.List;

public class PemjaReader implements Reader {

  private final List<Object> data;

  private final int batchSize;

  private final Header header;

  private final List<Row> rowList;

  private int offset = 0;

  public PemjaReader(List<Object> data, int batchSize) {
    this.data = data;
    this.batchSize = batchSize;

    this.header = getHeaderFromData();
    this.rowList = getRowListFromData();
  }

  private Header getHeaderFromData() {
    List<Object> firstRow;
    if (isList(data.get(0))) {
      firstRow = (List<Object>) data.get(0);
    } else {
      firstRow = data;
    }

    List<Field> fieldList = new ArrayList<>();
    for (Object fieldName : firstRow) {
      fieldList.add(new Field((String) fieldName, DataType.BINARY));
    }
    return new Header(fieldList);
  }

  private List<Row> getRowListFromData() {
    List<Row> rowList = new ArrayList<>();

    boolean is2DList = isList(data.get(0));
    if (is2DList) {
      for (int i = 1; i < data.size(); i++) {
        rowList.add(new Row(header, ((List<Object>) data.get(i)).toArray()));
      }
    }
    return rowList;
  }

  private boolean isList(Object object) {
    if (object == null) {
      return false;
    }
    return object instanceof List<?>;
  }

  @Override
  public boolean hasNextBatch() {
    return offset < rowList.size();
  }

  @Override
  public BatchData loadNextBatch() {
    BatchData batchData = new BatchData(header);
    int countDown = batchSize;
    while (countDown > 0 && offset < rowList.size()) {
      batchData.appendRow(rowList.get(offset));
      countDown--;
      offset++;
    }
    return batchData;
  }

  @Override
  public void close() {
    rowList.clear();
  }
}
