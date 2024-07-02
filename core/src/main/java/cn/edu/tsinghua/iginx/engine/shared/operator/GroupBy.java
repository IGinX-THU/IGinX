package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.function.FunctionCall;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import java.util.ArrayList;
import java.util.List;

public class GroupBy extends AbstractUnaryOperator {

  private final List<String> groupByCols;

  private final List<FunctionCall> functionCallList;

  public GroupBy(Source source, List<String> groupByCols, List<FunctionCall> functionCallList) {
    super(OperatorType.GroupBy, source);
    if (groupByCols == null || groupByCols.isEmpty()) {
      throw new IllegalArgumentException("groupByCols shouldn't be null");
    }
    this.groupByCols = groupByCols;
    this.functionCallList = functionCallList;
  }

  public List<String> getGroupByCols() {
    return groupByCols;
  }

  public List<FunctionCall> getFunctionCallList() {
    return functionCallList;
  }

  @Override
  public Operator copy() {
    return new GroupBy(
        getSource().copy(), new ArrayList<>(groupByCols), new ArrayList<>(functionCallList));
  }

  @Override
  public UnaryOperator copyWithSource(Source source) {
    return new GroupBy(source, new ArrayList<>(groupByCols), new ArrayList<>(functionCallList));
  }

  public boolean isDistinct() {
    // 所有的functionCall的distinct属性都相同，所以只需要看第一个就可以了
    if (functionCallList.size() > 0) {
      return functionCallList.get(0).getParams().isDistinct();
    }
    return false;
  }

  @Override
  public String getInfo() {
    StringBuilder builder = new StringBuilder();
    builder.append("GroupByCols: ").append(String.join(",", groupByCols));
    if (functionCallList != null && !functionCallList.isEmpty()) {
      builder.append(", FuncList(Name, FuncType): ");
      for (FunctionCall functionCall : functionCallList) {
        builder.append(functionCall.getNameAndFuncTypeStr()).append(",");
      }
      builder.append(" MappingType: ");
      builder.append(functionCallList.get(0).getFunction().getMappingType());
      if (isDistinct()) {
        builder.append(" isDistinct: true");
      } else {
        builder.append(" isDistinct: false");
      }
    }
    return builder.toString();
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (object == null || getClass() != object.getClass()) {
      return false;
    }
    GroupBy that = (GroupBy) object;
    return groupByCols.equals(that.groupByCols) && functionCallList.equals(that.functionCallList);
  }
}
