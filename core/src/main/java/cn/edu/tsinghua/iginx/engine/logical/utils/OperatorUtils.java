package cn.edu.tsinghua.iginx.engine.logical.utils;

import static cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils.combineTwoFilter;
import static cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils.getAllPathsFromFilter;
import static cn.edu.tsinghua.iginx.engine.shared.Constants.KEY;
import static cn.edu.tsinghua.iginx.engine.shared.function.system.ArithmeticExpr.ARITHMETIC_EXPR;
import static cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType.isBinaryOperator;
import static cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType.isMultipleOperator;
import static cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType.isUnaryOperator;

import cn.edu.tsinghua.iginx.engine.shared.expr.BaseExpression;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionCall;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionParams;
import cn.edu.tsinghua.iginx.engine.shared.function.manager.FunctionManager;
import cn.edu.tsinghua.iginx.engine.shared.operator.AbstractJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.BinaryOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.CrossJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.Distinct;
import cn.edu.tsinghua.iginx.engine.shared.operator.GroupBy;
import cn.edu.tsinghua.iginx.engine.shared.operator.InnerJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.Join;
import cn.edu.tsinghua.iginx.engine.shared.operator.MarkJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.MultipleOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.OuterJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.PathUnion;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.engine.shared.operator.Rename;
import cn.edu.tsinghua.iginx.engine.shared.operator.Reorder;
import cn.edu.tsinghua.iginx.engine.shared.operator.RowTransform;
import cn.edu.tsinghua.iginx.engine.shared.operator.Select;
import cn.edu.tsinghua.iginx.engine.shared.operator.SetTransform;
import cn.edu.tsinghua.iginx.engine.shared.operator.SingleJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.UnaryOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.BoolFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.FilterType;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.JoinAlgType;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OuterJoinType;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import cn.edu.tsinghua.iginx.engine.shared.source.SourceType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class OperatorUtils {

  public static Operator unionOperators(List<Operator> operators) {
    if (operators == null || operators.isEmpty()) return null;
    if (operators.size() == 1) return operators.get(0);
    Operator union = operators.get(0);
    for (int i = 1; i < operators.size(); i++) {
      union = new PathUnion(new OperatorSource(union), new OperatorSource(operators.get(i)));
    }
    return union;
  }

  public static Operator joinOperatorsByTime(List<Operator> operators) {
    return joinOperators(operators, KEY);
  }

  public static Operator joinOperators(List<Operator> operators, String joinBy) {
    if (operators == null || operators.isEmpty()) return null;
    if (operators.size() == 1) return operators.get(0);
    Operator join = operators.get(0);
    for (int i = 1; i < operators.size(); i++) {
      join = new Join(new OperatorSource(join), new OperatorSource(operators.get(i)), joinBy);
    }
    return join;
  }

  public static List<String> findPathList(Operator operator) {
    List<Project> projectList = new ArrayList<>();
    findProjectOperators(projectList, operator);

    if (projectList.isEmpty()) {
      return new ArrayList<>();
    } else {
      return projectList.get(0).getPatterns();
    }
  }

  public static void findProjectOperators(List<Project> projectOperatorList, Operator operator) {
    if (operator.getType() == OperatorType.Project) {
      projectOperatorList.add((Project) operator);
      return;
    }

    // dfs to find project operator.
    if (OperatorType.isUnaryOperator(operator.getType())) {
      UnaryOperator unaryOp = (UnaryOperator) operator;
      Source source = unaryOp.getSource();
      if (source.getType() != SourceType.Fragment) {
        findProjectOperators(projectOperatorList, ((OperatorSource) source).getOperator());
      }
    } else if (OperatorType.isBinaryOperator(operator.getType())) {
      BinaryOperator binaryOperator = (BinaryOperator) operator;
      findProjectOperators(
          projectOperatorList, ((OperatorSource) binaryOperator.getSourceA()).getOperator());
      findProjectOperators(
          projectOperatorList, ((OperatorSource) binaryOperator.getSourceB()).getOperator());
    } else if (OperatorType.isMultipleOperator(operator.getType())) {
      MultipleOperator multipleOperator = (MultipleOperator) operator;
      List<Source> sources = multipleOperator.getSources();
      for (Source source : sources) {
        findProjectOperators(projectOperatorList, ((OperatorSource) source).getOperator());
      }
    }
  }

  public static void findSelectOperators(List<Select> selectOperatorList, Operator operator) {
    if (operator.getType() == OperatorType.Select) {
      selectOperatorList.add((Select) operator);
    }

    // dfs to find select operator.
    if (OperatorType.isUnaryOperator(operator.getType())) {
      UnaryOperator unaryOp = (UnaryOperator) operator;
      Source source = unaryOp.getSource();
      if (source.getType() != SourceType.Fragment) {
        findSelectOperators(selectOperatorList, ((OperatorSource) source).getOperator());
      }
    } else if (OperatorType.isBinaryOperator(operator.getType())) {
      BinaryOperator binaryOperator = (BinaryOperator) operator;
      findSelectOperators(
          selectOperatorList, ((OperatorSource) binaryOperator.getSourceA()).getOperator());
      findSelectOperators(
          selectOperatorList, ((OperatorSource) binaryOperator.getSourceB()).getOperator());
    } else if (OperatorType.isMultipleOperator(operator.getType())) {
      MultipleOperator multipleOperator = (MultipleOperator) operator;
      List<Source> sources = multipleOperator.getSources();
      for (Source source : sources) {
        findSelectOperators(selectOperatorList, ((OperatorSource) source).getOperator());
      }
    }
  }

  public static Operator translateApply(Operator root, List<String> correlatedVariables) {
    if (!OperatorType.isJoinOperator(root.getType())) {
      return root;
    }

    AbstractJoin apply = (AbstractJoin) root;
    Operator operatorB = ((OperatorSource) apply.getSourceB()).getOperator();
    // 如果apply算子的右子树中不再有关联变量，则停止下推
    if (!hasPaths(operatorB, correlatedVariables)) {
      return root;
    }
    AbstractJoin applyCopy = (AbstractJoin) apply.copy();

    Operator operatorA =
        new Project(applyCopy.getSourceA(), correlatedVariables, null, false, true);
    applyCopy.setSourceA(new OperatorSource(operatorA));
    applyCopy.setPrefixA(null);
    if (applyCopy.getType() == OperatorType.MarkJoin) {
      MarkJoin markJoin = (MarkJoin) applyCopy;
      if (markJoin.getFilter().getType().equals(FilterType.Bool)) {
        applyCopy =
            new CrossJoin(
                applyCopy.getSourceA(),
                applyCopy.getSourceB(),
                applyCopy.getPrefixA(),
                applyCopy.getPrefixB());
      } else {
        applyCopy =
            new InnerJoin(
                applyCopy.getSourceA(),
                applyCopy.getSourceB(),
                applyCopy.getPrefixA(),
                applyCopy.getPrefixB(),
                markJoin.getFilter(),
                new ArrayList<>());
        ((InnerJoin) applyCopy).reChooseJoinAlg();
      }
    }
    Operator right = pushDownApply(applyCopy, correlatedVariables);

    if (apply.getType() == OperatorType.CrossJoin) {
      apply =
          new InnerJoin(
              apply.getSourceA(),
              new OperatorSource(right),
              apply.getPrefixA(),
              apply.getPrefixB(),
              null,
              new ArrayList<>(),
              false,
              JoinAlgType.HashJoin,
              correlatedVariables);
    } else {
      apply.setSourceB(new OperatorSource(right));
      apply.setExtraJoinPrefix(correlatedVariables);
      apply.setJoinAlgType(JoinAlgType.HashJoin);
    }
    return apply;
  }

  private static Operator pushDownApply(Operator root, List<String> correlatedVariables) {
    if (!OperatorType.isJoinOperator(root.getType())) {
      return root;
    }

    AbstractJoin apply = (AbstractJoin) root;
    Operator operatorB = ((OperatorSource) apply.getSourceB()).getOperator();
    // 如果apply算子的右子树中不再有关联变量，则停止下推
    if (!hasPaths(operatorB, correlatedVariables)) {
      return root;
    }

    boolean aCorrelatedWithRoot, bCorrelatedWithRoot;
    switch (operatorB.getType()) {
      case Project:
        Project project = (Project) operatorB;
        if (project.getSource().getType().equals(SourceType.Fragment)) {
          return root;
        }
        apply.setSourceB(project.getSource());
        List<String> patternsAll = new ArrayList<>(correlatedVariables);
        patternsAll.addAll(project.getPatterns());
        root =
            new Project(
                new OperatorSource(pushDownApply(apply, correlatedVariables)),
                patternsAll,
                project.getTagFilter(),
                false,
                true);
        break;
      case Reorder:
        Reorder reorder = (Reorder) operatorB;
        apply.setSourceB(reorder.getSource());
        root = pushDownApply(apply, correlatedVariables);
        break;
      case Select:
        Select select = (Select) operatorB;
        apply.setSourceB(select.getSource());
        root =
            new Select(
                new OperatorSource(pushDownApply(apply, correlatedVariables)),
                select.getFilter(),
                select.getTagFilter());
        if (((OperatorSource) ((Select) root).getSource()).getOperator() == apply) {
          root = combineAdjacentSelectAndJoin((Select) root);
        }
        break;
      case RowTransform:
        RowTransform rowTransform = (RowTransform) operatorB;
        apply.setSourceB(rowTransform.getSource());
        List<FunctionCall> functionCallList = new ArrayList<>(rowTransform.getFunctionCallList());
        for (String correlatedVariable : correlatedVariables) {
          FunctionParams params = new FunctionParams(new BaseExpression(correlatedVariable));
          functionCallList.add(
              new FunctionCall(FunctionManager.getInstance().getFunction(ARITHMETIC_EXPR), params));
        }
        root =
            new RowTransform(
                new OperatorSource(pushDownApply(apply, correlatedVariables)), functionCallList);
        break;
      case SetTransform:
        SetTransform setTransform = (SetTransform) operatorB;
        Operator newOperatorA = new Distinct(apply.getSourceA(), Collections.singletonList("*"));
        apply.setSourceA(new OperatorSource(newOperatorA));

        apply.setSourceB(setTransform.getSource());
        if (apply.getType().equals(OperatorType.SingleJoin)) {
          SingleJoin singleJoin = (SingleJoin) apply;
          apply =
              new OuterJoin(
                  singleJoin.getSourceA(),
                  singleJoin.getSourceB(),
                  apply.getPrefixA(),
                  apply.getPrefixB(),
                  OuterJoinType.LEFT,
                  singleJoin.getFilter(),
                  new ArrayList<>(),
                  false,
                  singleJoin.getJoinAlgType(),
                  singleJoin.getExtraJoinPrefix());
        }
        root =
            new GroupBy(
                new OperatorSource(pushDownApply(apply, correlatedVariables)),
                correlatedVariables,
                Collections.singletonList(setTransform.getFunctionCall()));
        break;
      case GroupBy:
        GroupBy groupBy = (GroupBy) operatorB;
        apply.setSourceB(groupBy.getSource());
        List<String> groupByCols = groupBy.getGroupByCols();
        groupByCols.addAll(correlatedVariables);
        root =
            new GroupBy(
                new OperatorSource(pushDownApply(apply, correlatedVariables)),
                groupByCols,
                groupBy.getFunctionCallList());
        break;
      case Rename:
        Rename rename = (Rename) operatorB;
        apply.setSourceB(rename.getSource());
        List<String> ignorePatterns = rename.getIgnorePatterns();
        ignorePatterns.addAll(correlatedVariables);
        root =
            new Rename(
                new OperatorSource(pushDownApply(apply, correlatedVariables)),
                rename.getAliasMap(),
                ignorePatterns);
        break;
      case CrossJoin:
        CrossJoin crossJoin = (CrossJoin) operatorB;
        Operator operatorACrossJoin = ((OperatorSource) crossJoin.getSourceA()).getOperator();
        Operator operatorBCrossJoin = ((OperatorSource) crossJoin.getSourceB()).getOperator();
        aCorrelatedWithRoot = hasPaths(operatorACrossJoin, correlatedVariables);
        bCorrelatedWithRoot = hasPaths(operatorBCrossJoin, correlatedVariables);
        if (!aCorrelatedWithRoot && bCorrelatedWithRoot) {
          apply.setSourceB(crossJoin.getSourceB());
          apply.setPrefixB(crossJoin.getPrefixB());
          crossJoin.setSourceB(new OperatorSource(pushDownApply(apply, correlatedVariables)));
          root = crossJoin;
        } else if (aCorrelatedWithRoot && !bCorrelatedWithRoot) {
          apply.setSourceB(crossJoin.getSourceA());
          apply.setPrefixB(crossJoin.getPrefixA());
          crossJoin.setSourceA(new OperatorSource(pushDownApply(apply, correlatedVariables)));
          root = crossJoin;
        } else if (aCorrelatedWithRoot) {
          AbstractJoin applyCopy = (AbstractJoin) apply.copy();
          apply.setSourceB(crossJoin.getSourceA());
          apply.setPrefixB(crossJoin.getPrefixA());
          applyCopy.setSourceB(crossJoin.getSourceB());
          applyCopy.setPrefixB(crossJoin.getPrefixB());
          root =
              new InnerJoin(
                  new OperatorSource(pushDownApply(apply, correlatedVariables)),
                  new OperatorSource(pushDownApply(applyCopy, correlatedVariables)),
                  null,
                  null,
                  new BoolFilter(true),
                  new ArrayList<>(),
                  false,
                  JoinAlgType.HashJoin,
                  correlatedVariables);
        }
        break;
      case InnerJoin:
        InnerJoin innerJoin = (InnerJoin) operatorB;
        Operator operatorAInnerJoin = ((OperatorSource) innerJoin.getSourceA()).getOperator();
        Operator operatorBInnerJoin = ((OperatorSource) innerJoin.getSourceB()).getOperator();
        aCorrelatedWithRoot = hasPaths(operatorAInnerJoin, correlatedVariables);
        bCorrelatedWithRoot = hasPaths(operatorBInnerJoin, correlatedVariables);
        if (!aCorrelatedWithRoot && bCorrelatedWithRoot) {
          apply.setSourceB(innerJoin.getSourceB());
          apply.setPrefixB(innerJoin.getPrefixB());
          innerJoin.setSourceB(new OperatorSource(pushDownApply(apply, correlatedVariables)));
        } else if (aCorrelatedWithRoot && !bCorrelatedWithRoot) {
          apply.setSourceB(innerJoin.getSourceA());
          apply.setPrefixB(innerJoin.getPrefixA());
          innerJoin.setSourceA(new OperatorSource(pushDownApply(apply, correlatedVariables)));
        } else if (aCorrelatedWithRoot) {
          AbstractJoin applyCopy = (AbstractJoin) apply.copy();
          apply.setSourceB(innerJoin.getSourceA());
          apply.setPrefixB(innerJoin.getPrefixA());
          applyCopy.setSourceB(innerJoin.getSourceB());
          applyCopy.setPrefixB(innerJoin.getPrefixB());
          innerJoin.setSourceA(new OperatorSource(pushDownApply(apply, correlatedVariables)));
          innerJoin.setSourceB(new OperatorSource(pushDownApply(applyCopy, correlatedVariables)));
          innerJoin.setJoinAlgType(JoinAlgType.HashJoin);
          innerJoin.setExtraJoinPrefix(correlatedVariables);
        }
        root = innerJoin;
        break;
      case OuterJoin:
        OuterJoin outerJoin = (OuterJoin) operatorB;
        Operator operatorAOuterJoin = ((OperatorSource) outerJoin.getSourceA()).getOperator();
        Operator operatorBOuterJoin = ((OperatorSource) outerJoin.getSourceB()).getOperator();
        aCorrelatedWithRoot = hasPaths(operatorAOuterJoin, correlatedVariables);
        bCorrelatedWithRoot = hasPaths(operatorBOuterJoin, correlatedVariables);
        if (outerJoin.getOuterJoinType() == OuterJoinType.LEFT && !bCorrelatedWithRoot) {
          apply.setSourceB(outerJoin.getSourceA());
          apply.setPrefixB(outerJoin.getPrefixA());
          outerJoin.setSourceA(new OperatorSource(pushDownApply(apply, correlatedVariables)));
        } else if (outerJoin.getOuterJoinType() == OuterJoinType.RIGHT && !aCorrelatedWithRoot) {
          apply.setSourceB(outerJoin.getSourceB());
          apply.setPrefixB(outerJoin.getPrefixB());
          outerJoin.setSourceB(new OperatorSource(pushDownApply(apply, correlatedVariables)));
        } else {
          AbstractJoin applyCopy = (AbstractJoin) apply.copy();
          apply.setSourceB(outerJoin.getSourceA());
          apply.setPrefixB(outerJoin.getPrefixA());
          applyCopy.setSourceB(outerJoin.getSourceB());
          applyCopy.setPrefixB(outerJoin.getPrefixB());
          outerJoin.setSourceA(new OperatorSource(pushDownApply(apply, correlatedVariables)));
          outerJoin.setSourceB(new OperatorSource(pushDownApply(applyCopy, correlatedVariables)));
          outerJoin.setJoinAlgType(JoinAlgType.HashJoin);
          outerJoin.setExtraJoinPrefix(correlatedVariables);
        }
        root = outerJoin;
        break;
      case SingleJoin:
        SingleJoin singleJoin = (SingleJoin) operatorB;
        Operator operatorBSingleJoin = ((OperatorSource) singleJoin.getSourceB()).getOperator();
        bCorrelatedWithRoot = hasPaths(operatorBSingleJoin, correlatedVariables);
        if (!bCorrelatedWithRoot) {
          apply.setSourceB(singleJoin.getSourceA());
          apply.setPrefixB(null);
          singleJoin.setSourceA(new OperatorSource(pushDownApply(apply, correlatedVariables)));
        } else {
          AbstractJoin applyCopy = (AbstractJoin) apply.copy();
          apply.setSourceB(singleJoin.getSourceA());
          apply.setPrefixB(null);
          applyCopy.setSourceB(singleJoin.getSourceB());
          applyCopy.setPrefixB(null);
          singleJoin.setSourceA(new OperatorSource(pushDownApply(apply, correlatedVariables)));
          singleJoin.setSourceB(new OperatorSource(pushDownApply(applyCopy, correlatedVariables)));
          singleJoin.setJoinAlgType(JoinAlgType.HashJoin);
          singleJoin.setExtraJoinPrefix(correlatedVariables);
        }
        root = singleJoin;
        break;
      case MarkJoin:
        MarkJoin markJoin = (MarkJoin) operatorB;
        Operator operatorBMarkJoin = ((OperatorSource) markJoin.getSourceB()).getOperator();
        bCorrelatedWithRoot = hasPaths(operatorBMarkJoin, correlatedVariables);
        if (!bCorrelatedWithRoot) {
          apply.setSourceB(markJoin.getSourceA());
          apply.setPrefixB(null);
          markJoin.setSourceA(new OperatorSource(pushDownApply(apply, correlatedVariables)));
        } else {
          AbstractJoin applyCopy = (AbstractJoin) apply.copy();
          apply.setSourceB(markJoin.getSourceA());
          apply.setPrefixB(null);
          applyCopy.setSourceB(markJoin.getSourceB());
          applyCopy.setPrefixB(null);
          markJoin.setSourceA(new OperatorSource(pushDownApply(apply, correlatedVariables)));
          markJoin.setSourceB(new OperatorSource(pushDownApply(applyCopy, correlatedVariables)));
          markJoin.setJoinAlgType(JoinAlgType.HashJoin);
          markJoin.setExtraJoinPrefix(correlatedVariables);
        }
        root = markJoin;
        break;
      case Union:
      case Except:
      case Intersect:
        throw new RuntimeException("Correlated subquery is not supported to use set operator yet.");
      default:
        throw new RuntimeException("Unexpected operator type: " + operatorB.getType());
    }
    return root;
  }

  private static boolean hasPaths(Operator root, List<String> paths) {
    if (root.getType().equals(OperatorType.Select)) {
      return !Collections.disjoint(getAllPathsFromFilter(((Select) root).getFilter()), paths);
    }
    if (root.getType().equals(OperatorType.Project)) {
      Project project = (Project) root;
      if (project.getSource().getType().equals(SourceType.Fragment)) {
        return false;
      }
    }
    if (isUnaryOperator(root.getType())) {
      UnaryOperator unaryOperator = (UnaryOperator) root;
      return hasPaths(((OperatorSource) unaryOperator.getSource()).getOperator(), paths);
    } else if (isBinaryOperator(root.getType())) {
      BinaryOperator binaryOperator = (BinaryOperator) root;
      Operator operatorA = ((OperatorSource) binaryOperator.getSourceA()).getOperator();
      Operator operatorB = ((OperatorSource) binaryOperator.getSourceB()).getOperator();
      if (hasPaths(operatorA, paths)) {
        return true;
      } else {
        return hasPaths(operatorB, paths);
      }
    } else if (isMultipleOperator(root.getType())) {
      MultipleOperator multipleOperator = (MultipleOperator) root;
      List<Source> sources = multipleOperator.getSources();
      for (Source source : sources) {
        if (hasPaths(((OperatorSource) source).getOperator(), paths)) {
          return true;
        }
      }
      return false;
    } else {
      throw new RuntimeException("Unexpected operator type: " + root.getType());
    }
  }

  private static Operator combineAdjacentSelectAndJoin(Select select) {
    Operator child = ((OperatorSource) select.getSource()).getOperator();
    if (!OperatorType.isJoinOperator(child.getType())) {
      throw new RuntimeException("Unexpected operator type: " + child.getType());
    }

    Filter newFilter;
    switch (child.getType()) {
      case CrossJoin:
        CrossJoin crossJoin = (CrossJoin) child;
        JoinAlgType algType = JoinAlgType.NestedLoopJoin;
        if (!crossJoin.getExtraJoinPrefix().isEmpty()) {
          algType = JoinAlgType.HashJoin;
        }
        return new InnerJoin(
            crossJoin.getSourceA(),
            crossJoin.getSourceB(),
            crossJoin.getPrefixA(),
            crossJoin.getPrefixB(),
            select.getFilter(),
            new ArrayList<>(),
            false,
            algType,
            crossJoin.getExtraJoinPrefix());
      case InnerJoin:
        InnerJoin innerJoin = (InnerJoin) child;
        newFilter = combineTwoFilter(innerJoin.getFilter(), select.getFilter());
        innerJoin.setFilter(newFilter);
        innerJoin.reChooseJoinAlg();
        return innerJoin;
      case OuterJoin:
        OuterJoin outerJoin = (OuterJoin) child;
        newFilter = combineTwoFilter(outerJoin.getFilter(), select.getFilter());
        outerJoin.setFilter(newFilter);
        outerJoin.reChooseJoinAlg();
        return outerJoin;
      case SingleJoin:
        SingleJoin singleJoin = (SingleJoin) child;
        newFilter = combineTwoFilter(singleJoin.getFilter(), select.getFilter());
        singleJoin.setFilter(newFilter);
        singleJoin.reChooseJoinAlg();
        return singleJoin;
      case MarkJoin:
        MarkJoin markJoin = (MarkJoin) child;
        newFilter = combineTwoFilter(markJoin.getFilter(), select.getFilter());
        markJoin.setFilter(newFilter);
        markJoin.reChooseJoinAlg();
        return markJoin;
      default:
        throw new RuntimeException("Unexpected operator type: " + child.getType());
    }
  }
}
