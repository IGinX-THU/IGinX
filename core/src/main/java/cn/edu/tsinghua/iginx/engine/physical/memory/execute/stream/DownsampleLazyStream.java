package cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream;

import static cn.edu.tsinghua.iginx.engine.shared.Constants.WINDOW_END_COL;
import static cn.edu.tsinghua.iginx.engine.shared.Constants.WINDOW_START_COL;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalTaskExecuteFailureException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.RowUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStreamWrapper;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionCall;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionParams;
import cn.edu.tsinghua.iginx.engine.shared.function.SetMappingFunction;
import cn.edu.tsinghua.iginx.engine.shared.operator.Downsample;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.ArrayList;
import java.util.List;

public class DownsampleLazyStream extends UnaryLazyStream {

  private final RowStreamWrapper wrapper;

  private final Downsample downsample;

  private final List<FunctionCall> functionCallList;

  private Row nextTarget;

  private boolean hasInitialized = false;

  private Header header;

  public DownsampleLazyStream(Downsample downsample, RowStream stream) {
    super(stream);
    this.wrapper = new RowStreamWrapper(stream);
    this.downsample = downsample;
    this.functionCallList = downsample.getFunctionCallList();
  }

  private void initialize() throws PhysicalException {
    if (hasInitialized) {
      return;
    }
    nextTarget = loadNext();
    if (nextTarget != null) {
      header = nextTarget.getHeader();
    }
    hasInitialized = true;
  }

  @Override
  public Header getHeader() throws PhysicalException {
    if (!hasInitialized) {
      initialize();
    }
    if (header == null) {
      header = Header.EMPTY_HEADER;
    }
    return header;
  }

  private Row loadNext() throws PhysicalException {
    if (nextTarget != null) {
      return nextTarget;
    }
    Row row = null;
    long windowStartKey = 0;
    long bias = downsample.getKeyRange().getActualBeginKey();
    long endKey = downsample.getKeyRange().getActualEndKey();
    long precision = downsample.getPrecision();
    long slideDistance = downsample.getSlideDistance();
    // startKey + (n - 1) * slideDistance + precision - 1 >= endKey
    int n = (int) (Math.ceil((double) (endKey - bias - precision + 1) / slideDistance) + 1);
    while (row == null && wrapper.hasNext()) {
      windowStartKey = wrapper.nextTimestamp() - (wrapper.nextTimestamp() - bias) % precision;
      List<Row> rows = new ArrayList<>();
      while (wrapper.hasNext() && wrapper.nextTimestamp() < windowStartKey + precision) {
        rows.add(wrapper.next());
      }
      Table table = new Table(rows.get(0).getHeader(), rows);

      List<Row> subRowList = new ArrayList<>();
      for (FunctionCall functionCall : functionCallList) {
        FunctionParams params = functionCall.getParams();
        SetMappingFunction function = (SetMappingFunction) functionCall.getFunction();
        try {
          subRowList.add(function.transform(table, params));
        } catch (Exception e) {
          throw new PhysicalTaskExecuteFailureException(
              "encounter error when execute set mapping function " + function.getIdentifier() + ".",
              e);
        }
      }
      row = RowUtils.combineMultipleColumns(subRowList);
    }
    if (row == null) {
      return null;
    } else {
      List<Field> fields = row.getHeader().getFields();
      fields.add(0, new Field(WINDOW_START_COL, DataType.LONG));
      fields.add(1, new Field(WINDOW_END_COL, DataType.LONG));
      Object[] values = new Object[row.getValues().length + 2];
      values[0] = windowStartKey;
      values[1] = windowStartKey + precision - 1;
      System.arraycopy(row.getValues(), 0, values, 2, row.getValues().length);
      return new Row(new Header(Field.KEY, fields), windowStartKey, values);
    }
  }

  @Override
  public boolean hasNext() throws PhysicalException {
    if (!hasInitialized) {
      initialize();
    }
    if (nextTarget == null) {
      nextTarget = loadNext();
    }
    return nextTarget != null;
  }

  @Override
  public Row next() throws PhysicalException {
    if (!hasNext()) {
      throw new IllegalStateException("row stream doesn't have more data!");
    }
    Row row = nextTarget;
    nextTarget = null;
    return row;
  }
}
