package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.function.FunctionCall;
import cn.edu.tsinghua.iginx.engine.shared.function.MappingType;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import java.util.ArrayList;
import java.util.List;

public class MappingTransform extends AbstractUnaryOperator {

  private final List<FunctionCall> functionCallList;

  public MappingTransform(Source source, List<FunctionCall> functionCallList) {
    super(OperatorType.MappingTransform, source);
    if (functionCallList == null || functionCallList.isEmpty()) {
      throw new IllegalArgumentException("functionCallList shouldn't be null or empty");
    }
    for (FunctionCall functionCall : functionCallList) {
      if (functionCall == null || functionCall.getFunction() == null) {
        throw new IllegalArgumentException("function shouldn't be null");
      }
      if (functionCall.getFunction().getMappingType() != MappingType.Mapping) {
        throw new IllegalArgumentException("function should be mapping function");
      }
    }
    this.functionCallList = functionCallList;
  }

  public List<FunctionCall> getFunctionCallList() {
    return functionCallList;
  }

  @Override
  public Operator copy() {
    return new MappingTransform(getSource().copy(), new ArrayList<>(functionCallList));
  }

  @Override
  public UnaryOperator copyWithSource(Source source) {
    return new MappingTransform(source, new ArrayList<>(functionCallList));
  }

  @Override
  public String getInfo() {
    StringBuilder sb = new StringBuilder();
    sb.append("FuncList(Name, FuncType): ");
    for (FunctionCall functionCall : functionCallList) {
      sb.append(functionCall.getNameAndFuncTypeStr());
      sb.append(", ");
    }

    sb.append("MappingType: Mapping");

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
    MappingTransform that = (MappingTransform) object;
    return functionCallList.equals(that.functionCallList);
  }
}
