package cn.edu.tsinghua.iginx.logical.optimizer.rules;

import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.engine.shared.source.SourceType;
import cn.edu.tsinghua.iginx.logical.optimizer.core.RuleCall;
import com.google.auto.service.AutoService;

@AutoService(Rule.class)
public class SingleJoinEliminateRule extends Rule {

  public SingleJoinEliminateRule() {
    /*
     * we want to match the topology like:
     *      Single Join
     *       /       \
     *    any        AbstractUnaryOperator
     */
    super(
        "SingleJoinEliminateRule",
        "FilterPushDownRule",
        operand(SingleJoin.class, any(), operand(AbstractUnaryOperator.class, any())));
  }

  @Override
  public boolean matches(RuleCall call) {
    SingleJoin join = (SingleJoin) call.getMatchedRoot();

    AbstractUnaryOperator expectSingleRow =
        (AbstractUnaryOperator) ((OperatorSource) join.getSourceB()).getOperator();
    return isSingleRowOperator(expectSingleRow);
  }

  @Override
  public void onMatch(RuleCall call) {
    SingleJoin singleJoin = (SingleJoin) call.getMatchedRoot();

    CrossJoin newJoin =
        new CrossJoin(
            singleJoin.getSourceA(),
            singleJoin.getSourceB(),
            singleJoin.getPrefixA(),
            singleJoin.getPrefixB(),
            singleJoin.getExtraJoinPrefix());

    Select newRoot =
        new Select(new OperatorSource(newJoin), singleJoin.getFilter(), singleJoin.getTagFilter());

    call.transformTo(newRoot);
  }

  private static boolean isSingleRowOperator(AbstractUnaryOperator operator) {
    switch (operator.getType()) {
      case SetTransform:
        return true;
      case Project:
      case Select:
      case Sort:
      case Limit:
      case Downsample:
      case RowTransform:
      case Rename:
      case Reorder:
      case AddSchemaPrefix:;
      case GroupBy:
      case Distinct:
      case AddSequence:
      case RemoveNullColumn:
        if (operator.getSource().getType() != SourceType.Operator) {
          return false;
        }
        OperatorSource source = (OperatorSource) operator.getSource();
        if (!(source.getOperator() instanceof AbstractUnaryOperator)) {
          return false;
        }
        return isSingleRowOperator((AbstractUnaryOperator) source.getOperator());
      default:
        return false;
    }
  }
}
