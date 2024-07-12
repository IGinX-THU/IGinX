package cn.edu.tsinghua.iginx.logical.optimizer.rules;

import cn.edu.tsinghua.iginx.engine.logical.utils.OperatorUtils;
import cn.edu.tsinghua.iginx.engine.shared.Constants;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.visitor.DeepFirstQueueVisitor;
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import cn.edu.tsinghua.iginx.engine.shared.source.SourceType;
import cn.edu.tsinghua.iginx.logical.optimizer.core.RuleCall;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import com.google.auto.service.AutoService;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@AutoService(Rule.class)
public class SetTransformPushDownPathUnionJoinRule extends Rule {

  public SetTransformPushDownPathUnionJoinRule() {
    /*
     * we want to match the topology like:
     *           SetTransform
     *            |
     *           PathUnion/Join
     *
     */
    super(
        SetTransformPushDownPathUnionJoinRule.class.getSimpleName(),
        operand(SetTransform.class, operand(AbstractBinaryOperator.class, any(), any())));
  }

  public boolean matches(RuleCall call) {
    SetTransform setTransform = (SetTransform) call.getMatchedRoot();

    List<Project> projects = new ArrayList<>();
    OperatorUtils.findProjectOperators(projects, setTransform);

    List<FragmentMeta> fragments =
        projects.stream()
            .map(AbstractUnaryOperator::getSource)
            .filter(source -> source.getType() == SourceType.Fragment)
            .map(source -> (FragmentSource) source)
            .map(FragmentSource::getFragment)
            .collect(Collectors.toList());

    // Check if each column can only appear once
    RangeSet<String> columnRangeSet = TreeRangeSet.create();
    for (FragmentMeta fragment : fragments) {
      // if there is a dummy fragment, we can't push down the set transform correctly
      if (fragment.isDummyFragment()) {
        return false;
      }
      ColumnsInterval columnsInterval = fragment.getColumnsInterval();
      // TODO: Would schema prefix affect the result?
      if (columnsInterval.getSchemaPrefix() != null) {
        return false;
      }
      String startColumn = columnsInterval.getStartColumn();
      String endColumn = columnsInterval.getEndColumn();
      if (startColumn == null || endColumn == null) {}

      Range<String> columnRange = rangeOf(columnsInterval);
      // if there is an intersection of fragment columnIntervals, maybe there are intersecting
      // columns in Join
      if (columnRangeSet.intersects(columnRange)) {
        return false;
      }
      columnRangeSet.add(columnRange);
    }

    Source source = setTransform.getSource();
    if (source.getType() != SourceType.Operator) {
      return false;
    }

    DeepFirstQueueVisitor deepFirstQueueVisitor = new DeepFirstQueueVisitor();
    ((OperatorSource) source).getOperator().accept(deepFirstQueueVisitor);

    for (Operator operator : deepFirstQueueVisitor.getQueue()) {
      if (operator instanceof Project) {
        Source projectSource = ((Project) operator).getSource();
        if (projectSource.getType() != SourceType.Fragment) {
          return false;
        }
      } else if (operator instanceof Join) {
        String joinBy = ((Join) operator).getJoinBy();
        switch (joinBy) {
          case Constants.KEY:
          case Constants.ORDINAL:
            break;
          default:
            return false;
        }
      } else if (operator instanceof PathUnion) {
        // do nothing
      } else {
        return false;
      }
    }

    return true;
  }

  private static Range<String> rangeOf(ColumnsInterval columnsInterval) {
    String startColumn = columnsInterval.getStartColumn();
    String endColumn = columnsInterval.getEndColumn();
    if (startColumn == null && endColumn == null) {
      return Range.all();
    } else if (startColumn == null) {
      return Range.lessThan(endColumn);
    } else if (endColumn == null) {
      return Range.atLeast(startColumn);
    } else {
      return Range.closedOpen(startColumn, endColumn);
    }
  }

  @Override
  public void onMatch(RuleCall call) {
    SetTransform setTransform = (SetTransform) call.getMatchedRoot();
    List<Project> projectList = new ArrayList<>();
    OperatorUtils.findProjectOperators(projectList, setTransform);

    List<SetTransform> setTransformList =
        projectList.stream()
            .map(Project::copy)
            .map(OperatorSource::new)
            .map(setTransform::copyWithSource)
            .map(SetTransform.class::cast)
            .collect(Collectors.toList());

    Operator newRoot = OperatorUtils.joinOperators(setTransformList, Constants.ORDINAL);
    call.transformTo(newRoot);
  }
}
