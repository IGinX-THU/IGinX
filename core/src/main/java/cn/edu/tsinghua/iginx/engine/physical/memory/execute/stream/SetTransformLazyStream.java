package cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream;

import static cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.RowUtils.combineMultipleColumns;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalTaskExecuteFailureException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionCall;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionParams;
import cn.edu.tsinghua.iginx.engine.shared.function.SetMappingFunction;
import cn.edu.tsinghua.iginx.engine.shared.operator.SetTransform;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SetTransformLazyStream extends UnaryLazyStream {

  private final List<FunctionCall> functionCallList;

  private Row nextRow;

  private Map<List<String>, RowStream> distinctStreamMap; // 用于存储distinct处理过的stream，其中null指向原始stream

  private boolean hasConsumed = false;

  public SetTransformLazyStream(
      SetTransform setTransform, Map<List<String>, RowStream> distinctStreamMap) {
    super(distinctStreamMap.get(null));
    this.distinctStreamMap = distinctStreamMap;
    this.functionCallList = setTransform.getFunctionCallList();
  }

  @Override
  public Header getHeader() throws PhysicalException {
    if (!hasConsumed && nextRow == null) { // 没有被消费过并且当前值为空，实际上表示当前值还没有被计算过
      nextRow = calculate();
      hasConsumed = nextRow == null;
    }
    return nextRow == null ? Header.EMPTY_HEADER : nextRow.getHeader();
  }

  @Override
  public boolean hasNext() throws PhysicalException {
    if (!hasConsumed && nextRow == null) { // 没有被消费过并且当前值为空，实际上表示当前值还没有被计算过
      nextRow = calculate();
      hasConsumed = nextRow == null;
    }
    return !hasConsumed;
  }

  private Row calculate() throws PhysicalException {
    SetMappingFunction function = null;
    List<Row> rowList = new ArrayList<>();
    try {
      for (FunctionCall functionCall : functionCallList) {
        function = (SetMappingFunction) functionCall.getFunction();
        FunctionParams params = functionCall.getParams();
        if (params.isDistinct()) {
          rowList.add(function.transform((Table) distinctStreamMap.get(params.getPaths()), params));
        } else {
          rowList.add(function.transform((Table) stream, params));
        }
      }

    } catch (Exception e) {
      if (function != null) {
        throw new PhysicalTaskExecuteFailureException(
            "encounter error when execute set mapping function " + function.getIdentifier() + ".",
            e);
      } else {
        throw new PhysicalTaskExecuteFailureException(
            "encounter error when execute set mapping function.", e);
      }
    }
    return combineMultipleColumns(rowList);
  }

  @Override
  public Row next() throws PhysicalException {
    if (!hasNext()) {
      throw new IllegalStateException("row stream doesn't have more data!");
    }
    hasConsumed = true;
    return nextRow;
  }
}
