package cn.edu.tsinghua.iginx.engine.logical.utils;

import static cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils.getAllPathsFromFilter;
import static cn.edu.tsinghua.iginx.engine.shared.Constants.KEY;
import static cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType.isBinaryOperator;
import static cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType.isMultipleOperator;
import static cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType.isUnaryOperator;

import cn.edu.tsinghua.iginx.engine.shared.operator.BinaryOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.CrossJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.GroupBy;
import cn.edu.tsinghua.iginx.engine.shared.operator.Join;
import cn.edu.tsinghua.iginx.engine.shared.operator.MultipleOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.engine.shared.operator.Select;
import cn.edu.tsinghua.iginx.engine.shared.operator.SetTransform;
import cn.edu.tsinghua.iginx.engine.shared.operator.UnaryOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.Union;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
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
            union = new Union(new OperatorSource(union), new OperatorSource(operators.get(i)));
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
        if (isUnaryOperator(operator.getType())) {
            UnaryOperator unaryOp = (UnaryOperator) operator;
            Source source = unaryOp.getSource();
            if (source.getType() != SourceType.Fragment) {
                findProjectOperators(projectOperatorList, ((OperatorSource) source).getOperator());
            }
        } else if (OperatorType.isBinaryOperator(operator.getType())) {
            BinaryOperator binaryOperator = (BinaryOperator) operator;
            findProjectOperators(
                    projectOperatorList,
                    ((OperatorSource) binaryOperator.getSourceA()).getOperator());
            findProjectOperators(
                    projectOperatorList,
                    ((OperatorSource) binaryOperator.getSourceB()).getOperator());
        } else {
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
            return;
        }

        // dfs to find select operator.
        if (isUnaryOperator(operator.getType())) {
            UnaryOperator unaryOp = (UnaryOperator) operator;
            Source source = unaryOp.getSource();
            if (source.getType() != SourceType.Fragment) {
                findSelectOperators(selectOperatorList, ((OperatorSource) source).getOperator());
            }
        } else if (OperatorType.isBinaryOperator(operator.getType())) {
            BinaryOperator binaryOperator = (BinaryOperator) operator;
            findSelectOperators(
                    selectOperatorList,
                    ((OperatorSource) binaryOperator.getSourceA()).getOperator());
            findSelectOperators(
                    selectOperatorList,
                    ((OperatorSource) binaryOperator.getSourceB()).getOperator());
        } else {
            MultipleOperator multipleOperator = (MultipleOperator) operator;
            List<Source> sources = multipleOperator.getSources();
            for (Source source : sources) {
                findSelectOperators(selectOperatorList, ((OperatorSource) source).getOperator());
            }
        }
    }

    public static Operator pushDownApply(
            Operator root, List<String> patternsLeft, List<String> correlatedVariables) {
        if (!OperatorType.isJoinOperator(root.getType())) {
            return root;
        }

        BinaryOperator apply = (BinaryOperator) root;
        Operator operatorB = ((OperatorSource) apply.getSourceB()).getOperator();
        if (!hasPaths(root, correlatedVariables)) {
            return root;
        }

        switch (operatorB.getType()) {
            case Project:
                Project project = (Project) operatorB;
                if (project.getSource().getType().equals(SourceType.Fragment)) {
                    return root;
                }
                apply.setSourceB(project.getSource());
                patternsLeft.addAll(project.getPatterns());
                root =
                        new Project(
                                new OperatorSource(
                                        pushDownApply(apply, patternsLeft, correlatedVariables)),
                                patternsLeft,
                                project.getTagFilter());
                break;
            case Select:
                Select select = (Select) operatorB;
                apply.setSourceB(select.getSource());
                root =
                        new Select(
                                new OperatorSource(
                                        pushDownApply(apply, patternsLeft, correlatedVariables)),
                                select.getFilter(),
                                select.getTagFilter());
                break;
                //            case RowTransform:
                //                RowTransform rowTransform = (RowTransform) operatorB;
                //                apply.setSourceB(rowTransform.getSource());
                //                List<FunctionCall> functionCallList =
                // rowTransform.getFunctionCallList();
            case SetTransform:
                SetTransform setTransform = (SetTransform) operatorB;
                apply.setSourceB(setTransform.getSource());
                root =
                        new GroupBy(
                                new OperatorSource(
                                        pushDownApply(apply, patternsLeft, correlatedVariables)),
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
                                new OperatorSource(
                                        pushDownApply(apply, patternsLeft, correlatedVariables)),
                                groupByCols,
                                groupBy.getFunctionCallList());
                break;
            case CrossJoin:
                CrossJoin crossJoin = (CrossJoin) operatorB;
                Operator operatorACrossJoin =
                        ((OperatorSource) crossJoin.getSourceA()).getOperator();
                Operator operatorBCrossJoin =
                        ((OperatorSource) crossJoin.getSourceB()).getOperator();
                boolean aCorrelatedWithRoot = hasPaths(operatorACrossJoin, correlatedVariables);
                boolean bCorrelatedWithRoot = hasPaths(operatorBCrossJoin, correlatedVariables);
                if (!aCorrelatedWithRoot && bCorrelatedWithRoot) {
                    apply.setSourceB(crossJoin.getSourceB());
                    root =
                            new CrossJoin(
                                    new OperatorSource(operatorACrossJoin),
                                    new OperatorSource(
                                            pushDownApply(
                                                    apply, patternsLeft, correlatedVariables)),
                                    crossJoin.getPrefixA(),
                                    crossJoin.getPrefixB());
                } else if (aCorrelatedWithRoot && !bCorrelatedWithRoot) {
                    apply.setSourceB(crossJoin.getSourceA());
                    root =
                            new CrossJoin(
                                    new OperatorSource(
                                            pushDownApply(
                                                    apply, patternsLeft, correlatedVariables)),
                                    new OperatorSource(operatorBCrossJoin),
                                    crossJoin.getPrefixA(),
                                    crossJoin.getPrefixB());
                } else if (aCorrelatedWithRoot) {
                    BinaryOperator applyCopy = (BinaryOperator) apply.copy();
                    apply.setSourceB(crossJoin.getSourceA());
                    applyCopy.setSourceB(crossJoin.getSourceB());
                    root =
                            new CrossJoin(
                                    new OperatorSource(
                                            pushDownApply(
                                                    apply, patternsLeft, correlatedVariables)),
                                    new OperatorSource(
                                            pushDownApply(
                                                    applyCopy, patternsLeft, correlatedVariables)),
                                    crossJoin.getPrefixA(),
                                    crossJoin.getPrefixB());
                }
                break;
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
}
