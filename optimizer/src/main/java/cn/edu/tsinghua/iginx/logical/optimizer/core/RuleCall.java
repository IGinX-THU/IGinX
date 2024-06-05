package cn.edu.tsinghua.iginx.logical.optimizer.core;

import cn.edu.tsinghua.iginx.engine.shared.operator.BinaryOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.MultipleOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.UnaryOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import cn.edu.tsinghua.iginx.logical.optimizer.rbo.RuleBasedPlanner;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class RuleCall {

  private static final Logger LOGGER = LoggerFactory.getLogger(RuleCall.class);

  private final Operator matchedRoot;

  private final Map<Operator, Operator> parentIndexMap;

  private final Map<Operator, List<Operator>> childrenIndex;

  private Object context;

  private final RuleBasedPlanner planner;

  public RuleCall(
      Operator matchedRoot,
      Map<Operator, Operator> parentIndexMap,
      Map<Operator, List<Operator>> childrenIndex,
      RuleBasedPlanner planner) {
    this.matchedRoot = matchedRoot;
    this.parentIndexMap = parentIndexMap;
    this.childrenIndex = childrenIndex;
    this.planner = planner;
  }

  public Operator getMatchedRoot() {
    return matchedRoot;
  }

  public Map<Operator, List<Operator>> getChildrenIndex() {
    return childrenIndex;
  }

  public Map<Operator, Operator> getParentIndexMap() {
    return parentIndexMap;
  }

  public void transformTo(Operator newRoot) {
    Operator parent = parentIndexMap.get(matchedRoot);
    if (parent == null) {
      planner.setRoot(newRoot);
      return;
    }

    // replace topology
    OperatorType parentType = parent.getType();
    if (OperatorType.isUnaryOperator(parentType)) {
      UnaryOperator unaryOperator = (UnaryOperator) parent;
      unaryOperator.setSource(new OperatorSource(newRoot));
    } else if (OperatorType.isBinaryOperator(parentType)) {
      BinaryOperator binaryOperator = (BinaryOperator) parent;
      Operator childA = ((OperatorSource) binaryOperator.getSourceA()).getOperator();
      Operator childB = ((OperatorSource) binaryOperator.getSourceB()).getOperator();
      if (childA == matchedRoot) {
        binaryOperator.setSourceA(new OperatorSource(newRoot));
      } else if (childB == matchedRoot) {
        binaryOperator.setSourceB(new OperatorSource(newRoot));
      } else {
        LOGGER.error("child and parent mismatch.");
      }
    } else {
      MultipleOperator multipleOp = (MultipleOperator) parent;
      int childIndex = -1;
      List<Source> sourceList = multipleOp.getSources();
      for (int i = 0; i < sourceList.size(); i++) {
        Operator child = ((OperatorSource) sourceList.get(i)).getOperator();
        if (child == matchedRoot) {
          childIndex = i;
          break;
        }
      }
      if (childIndex == -1) {
        LOGGER.error("child and parent mismatch.");
      } else {
        multipleOp.getSources().set(childIndex, new OperatorSource(newRoot));
      }
    }
  }

  public void setContext(Object context) {
    this.context = context;
  }

  public Object getContext() {
    return context;
  }
}
