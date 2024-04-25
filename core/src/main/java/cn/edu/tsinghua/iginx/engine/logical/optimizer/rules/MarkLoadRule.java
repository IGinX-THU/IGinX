package cn.edu.tsinghua.iginx.engine.logical.optimizer.rules;

import cn.edu.tsinghua.iginx.engine.logical.optimizer.core.RuleCall;

public class MarkLoadRule extends Rule {

  public static final class InstanceHolder {
    static final MarkLoadRule INSTANCE = new MarkLoadRule();
  }

  public static MarkLoadRule getInstance() {
    return InstanceHolder.INSTANCE;
  }

  private MarkLoadRule() {
    super("MarkLoadRule", any());
  }

  @Override
  public boolean matches(RuleCall call) {
    switch (call.getMatchedRoot().getType()) {
      case GroupBy:
      case Join:
      case InnerJoin:
      case OuterJoin:
      case CrossJoin:
      case MarkJoin:
        return true;
      default:
        return false;
    }
  }

  @Override
  public void onMatch(RuleCall call) {
    //    Operator matchedRoot = call.getMatchedRoot();
    //    if (matchedRoot instanceof UnaryOperator) {
    //      // deal with GroupBy
    //      UnaryOperator unaryOp = (UnaryOperator) matchedRoot;
    //      Source newSource = new OperatorSource(new Load(unaryOp.getSource()));
    //      unaryOp.setSource(newSource);
    //    } else if (matchedRoot instanceof BinaryOperator) {
    //      // deal with Join
    //      BinaryOperator binOp = (BinaryOperator) matchedRoot;
    //      Source newSourceA = new OperatorSource(new Load(binOp.getSourceA()));
    //      Source newSourceB = new OperatorSource(new Load(binOp.getSourceB()));
    //      binOp.setSourceA(newSourceA);
    //      binOp.setSourceB(newSourceB);
    //    }
  }
}
