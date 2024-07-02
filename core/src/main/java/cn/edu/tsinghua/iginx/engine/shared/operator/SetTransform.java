package cn.edu.tsinghua.iginx.engine.shared.operator;

import static cn.edu.tsinghua.iginx.engine.shared.function.FunctionUtils.isCanUseSetQuantifierFunction;

import cn.edu.tsinghua.iginx.engine.shared.function.FunctionCall;
import cn.edu.tsinghua.iginx.engine.shared.function.MappingType;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import java.util.ArrayList;
import java.util.List;

public class SetTransform extends AbstractUnaryOperator {

  private final List<FunctionCall> functionCallList;

  public SetTransform(Source source, List<FunctionCall> functionCallList) {
    super(OperatorType.SetTransform, source);
    this.functionCallList = new ArrayList<>();

    if (functionCallList == null) {
      throw new IllegalArgumentException("functionCallList shouldn't be null or empty");
    }

    for (FunctionCall functionCall : functionCallList) {
      if (functionCall.getParams().isDistinct()
          != functionCallList.get(0).getParams().isDistinct()) {
        throw new IllegalArgumentException(
            "functionCallList should have the same distinct property");
      }

      if (functionCall == null || functionCall.getFunction() == null) {
        throw new IllegalArgumentException("function shouldn't be null");
      }
      if (functionCall.getFunction().getMappingType() != MappingType.SetMapping) {
        throw new IllegalArgumentException("function should be set mapping function");
      }
      this.functionCallList.add(functionCall);
      if (isDistinct()
          && !isCanUseSetQuantifierFunction(functionCall.getFunction().getIdentifier())) {
        throw new IllegalArgumentException(
            "function " + functionCall.getFunction().getIdentifier() + " can't use DISTINCT");
      }
    }
  }

  public List<FunctionCall> getFunctionCallList() {
    return functionCallList;
  }

  public boolean isDistinct() {
    // 所有的functionCall的distinct属性都相同，所以只需要看第一个就可以了
    if (functionCallList.size() > 0) {
      return functionCallList.get(0).getParams().isDistinct();
    }
    return false;
  }

  @Override
  public Operator copy() {
    return new SetTransform(getSource().copy(), new ArrayList<>(functionCallList));
  }

  @Override
  public UnaryOperator copyWithSource(Source source) {
    return new SetTransform(source, new ArrayList<>(functionCallList));
  }

  @Override
  public String getInfo() {
    StringBuilder sb = new StringBuilder();
    sb.append("FuncList(Name, FuncType): ");
    for (FunctionCall functionCall : functionCallList) {
      sb.append(functionCall.getNameAndFuncTypeStr());
      sb.append(", ");
    }

    sb.append("MappingType: SetMapping, ");

    if (isDistinct()) {
      sb.append("isDistinct: true");
    } else {
      sb.append("isDistinct: false");
    }

    return sb.toString();
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (object == null || getClass() != object.getClass()) {
      return false;
    }
    SetTransform that = (SetTransform) object;
    return functionCallList.equals(that.functionCallList);
  }
}
