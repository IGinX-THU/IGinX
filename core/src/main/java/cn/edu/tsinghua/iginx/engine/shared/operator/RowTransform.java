package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.function.FunctionCall;
import cn.edu.tsinghua.iginx.engine.shared.function.MappingType;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import java.util.ArrayList;
import java.util.List;

public class RowTransform extends AbstractUnaryOperator {

  private final List<FunctionCall> functionCallList;

  public RowTransform(Source source, List<FunctionCall> functionCallList) {
    super(OperatorType.RowTransform, source);
    this.functionCallList = new ArrayList<>();
    functionCallList.forEach(
        functionCall -> {
          if (functionCall == null || functionCall.getFunction() == null) {
            throw new IllegalArgumentException("function shouldn't be null");
          }
          if (functionCall.getFunction().getMappingType() != MappingType.RowMapping) {
            throw new IllegalArgumentException("function should be set mapping function");
          }
          this.functionCallList.add(functionCall);
        });
  }

  public List<FunctionCall> getFunctionCallList() {
    return functionCallList;
  }

  @Override
  public Operator copy() {
    return new RowTransform(getSource().copy(), new ArrayList<>(functionCallList));
  }

  @Override
  public UnaryOperator copyWithSource(Source source) {
    return new RowTransform(source, new ArrayList<>(functionCallList));
  }

  public String getInfo() {
    StringBuilder sb = new StringBuilder();
    sb.append("FuncList(Name, FuncType): ");
    for (FunctionCall functionCall : functionCallList) {
      sb.append(functionCall.getNameAndFuncTypeStr());
      sb.append(", ");
    }

    sb.append("MappingType: RowMapping");

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
    RowTransform that = (RowTransform) object;
    return functionCallList.equals(that.functionCallList);
  }
}
