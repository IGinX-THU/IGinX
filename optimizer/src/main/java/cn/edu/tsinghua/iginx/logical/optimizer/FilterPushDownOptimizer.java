package cn.edu.tsinghua.iginx.logical.optimizer;

import cn.edu.tsinghua.iginx.engine.logical.optimizer.Optimizer;
import cn.edu.tsinghua.iginx.engine.logical.utils.LogicalFilterUtils;
import cn.edu.tsinghua.iginx.engine.logical.utils.OperatorUtils;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import cn.edu.tsinghua.iginx.engine.shared.source.SourceType;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilterPushDownOptimizer implements Optimizer {

  private static final Logger LOGGER = LoggerFactory.getLogger(FilterPushDownOptimizer.class);

  private static FilterPushDownOptimizer instance;

  public static FilterPushDownOptimizer getInstance() {
    if (instance == null) {
      synchronized (FilterFragmentOptimizer.class) {
        if (instance == null) {
          instance = new FilterPushDownOptimizer();
        }
      }
    }
    return instance;
  }

  @Override
  public Operator optimize(Operator root) {
    // only optimize query
    if (root.getType() == OperatorType.CombineNonQuery
        || root.getType() == OperatorType.ShowColumns) {
      return root;
    }

    List<Select> selectOperatorList = new ArrayList<>();
    OperatorUtils.findSelectOperators(selectOperatorList, root);

    if (selectOperatorList.isEmpty()) {
      LOGGER.info("There is no filter in logical tree.");
      return root;
    }

    for (Select selectOperator : selectOperatorList) {
      pushDown(selectOperator);
    }
    return root;
  }

  private void pushDown(Select selectOperator) {
    List<Pair<Project, Operator>> projectAndFatherOperatorList = new ArrayList<>();
    Stack<Operator> stack = new Stack<>();
    findProjectUpperFragment(projectAndFatherOperatorList, stack, selectOperator);

    if (projectAndFatherOperatorList.size() == 0) {
      LOGGER.error("There is no project operator just upper fragment in select tree.");
      return;
    }

    // 获取所有分片信息
    Set<FragmentMeta> fragmentMetaSet = new HashSet<>();
    for (Pair<Project, Operator> pair : projectAndFatherOperatorList) {
      Project project = pair.getK();
      FragmentMeta fragmentMeta = ((FragmentSource) project.getSource()).getFragment();
      fragmentMetaSet.add(fragmentMeta);
    }

    Map<String, Filter> cache = new HashMap<>();
    for (Pair<Project, Operator> pair : projectAndFatherOperatorList) {
      Filter filter = selectOperator.getFilter().copy();
      TagFilter tagFilter =
          selectOperator.getTagFilter() != null ? selectOperator.getTagFilter().copy() : null;

      Project project = pair.getK();
      FragmentMeta fragmentMeta = ((FragmentSource) project.getSource()).getFragment();

      // 考虑重命名的情况，向上找到Rename节点，并将filter中重命名后的Path替换为原始的path
      List<Operator> renameList = new ArrayList<>();
      getRenameOperator(selectOperator, project, renameList);
      Filter unrenamedFilter = filter.copy();
      for (Operator rename : renameList) {
        unrenamedFilter = replacePathByRenameMap(unrenamedFilter, ((Rename) rename).getAliasMap());
      }

      // the same meta just call once.
      Filter subFilter;
      if (cache.containsKey(fragmentMeta.getMasterStorageUnitId())) {
        subFilter = cache.get(fragmentMeta.getMasterStorageUnitId()).copy();
      } else {
        subFilter =
            LogicalFilterUtils.getSubFilterFromFragment(filter, fragmentMeta.getColumnsInterval());
        subFilter = LogicalFilterUtils.removeWildCardOrFilterByFragment(subFilter, fragmentMetaSet);
        cache.put(fragmentMeta.getMasterStorageUnitId(), subFilter);
      }
      subFilter = LogicalFilterUtils.removePathByPatterns(subFilter, project.getPatterns());

      if (subFilter.getType() == FilterType.Bool && ((BoolFilter) subFilter).isTrue()) {
        // need to scan whole scope.
        continue;
      }

      Select subSelect = new Select(new OperatorSource(project), subFilter, tagFilter);

      Operator fatherOperator = pair.getV();
      if (fatherOperator != null) {
        if (OperatorType.isUnaryOperator(fatherOperator.getType())) {
          UnaryOperator unaryOp = (UnaryOperator) fatherOperator;
          unaryOp.setSource(new OperatorSource(subSelect));
        } else if (OperatorType.isBinaryOperator(fatherOperator.getType())) {
          BinaryOperator binaryOperator = (BinaryOperator) fatherOperator;
          Operator operatorA = ((OperatorSource) binaryOperator.getSourceA()).getOperator();
          Operator operatorB = ((OperatorSource) binaryOperator.getSourceB()).getOperator();

          if (operatorA.equals(project)) {
            binaryOperator.setSourceA(new OperatorSource(subSelect));
          } else if (operatorB.equals(project)) {
            binaryOperator.setSourceB(new OperatorSource(subSelect));
          }
        } else if (OperatorType.isMultipleOperator(fatherOperator.getType())) {
          MultipleOperator multipleOperator = (MultipleOperator) fatherOperator;
          List<Source> sources = multipleOperator.getSources();

          int index = -1;
          for (int i = 0; i < sources.size(); i++) {
            Operator curOperator = ((OperatorSource) sources.get(i)).getOperator();
            if (curOperator.equals(project)) {
              index = i;
            }
          }
          if (index != -1) {
            sources.set(index, new OperatorSource(subSelect));
          }
          multipleOperator.setSources(sources);
        }

        // 在多filter情况下，如果将2个filter都下推到fragment，那么需要将2个filter合并，否则只会下推离fragment最近的filter。
        if (fatherOperator.getType() == OperatorType.Select
            && OperatorType.isUnaryOperator(selectOperator.getType())) {
          Select fatherSelect = (Select) fatherOperator;
          fatherSelect.setFilter(
              LogicalFilterUtils.mergeFilter(fatherSelect.getFilter(), subFilter));
          fatherSelect.setSource(new OperatorSource(project));
        }
      }
    }
  }

  private void findProjectUpperFragment(
      List<Pair<Project, Operator>> projectAndFatherOperatorList,
      Stack<Operator> stack,
      Operator operator) {
    // dfs to find project operator just upper fragment and his father operator.
    stack.push(operator);
    if (OperatorType.isUnaryOperator(operator.getType())) {
      UnaryOperator unaryOp = (UnaryOperator) operator;
      Source source = unaryOp.getSource();
      if (source.getType() == SourceType.Fragment) {
        Project project = (Project) stack.pop();
        Operator father = stack.isEmpty() ? null : stack.peek();
        projectAndFatherOperatorList.add(new Pair<>(project, father));
        return;
      } else {
        findProjectUpperFragment(
            projectAndFatherOperatorList, stack, ((OperatorSource) source).getOperator());
      }
    } else if (OperatorType.isBinaryOperator(operator.getType())) {
      BinaryOperator binaryOperator = (BinaryOperator) operator;
      findProjectUpperFragment(
          projectAndFatherOperatorList,
          stack,
          ((OperatorSource) binaryOperator.getSourceA()).getOperator());
      findProjectUpperFragment(
          projectAndFatherOperatorList,
          stack,
          ((OperatorSource) binaryOperator.getSourceB()).getOperator());
    } else if (OperatorType.isMultipleOperator(operator.getType())) {
      MultipleOperator multipleOperator = (MultipleOperator) operator;
      List<Source> sources = multipleOperator.getSources();
      for (Source source : sources) {
        findProjectUpperFragment(
            projectAndFatherOperatorList, stack, ((OperatorSource) source).getOperator());
      }
    }
    stack.pop();
  }

  /** DFS搜索，从起始节点到目标节点之间路径上的Rename节点，并将其加入List中。 */
  private boolean getRenameOperator(
      Operator curOperator, Operator target, List<Operator> renameOperatorList) {
    if (curOperator == target) {
      return true;
    }

    boolean isCorrectRoad = false;
    if (curOperator.getType() == OperatorType.Rename) {
      renameOperatorList.add(curOperator);
    }

    if (OperatorType.isUnaryOperator(curOperator.getType())) {
      UnaryOperator unaryOp = (UnaryOperator) curOperator;
      Source source = unaryOp.getSource();
      if (source.getType() != SourceType.Fragment) {
        isCorrectRoad =
            getRenameOperator(((OperatorSource) source).getOperator(), target, renameOperatorList);
      }
    } else if (OperatorType.isBinaryOperator(curOperator.getType())) {
      BinaryOperator binaryOperator = (BinaryOperator) curOperator;
      isCorrectRoad =
          getRenameOperator(
              ((OperatorSource) binaryOperator.getSourceA()).getOperator(),
              target,
              renameOperatorList);
      if (!isCorrectRoad) {
        isCorrectRoad =
            getRenameOperator(
                ((OperatorSource) binaryOperator.getSourceB()).getOperator(),
                target,
                renameOperatorList);
      }
    } else if (OperatorType.isMultipleOperator(curOperator.getType())) {
      MultipleOperator multipleOperator = (MultipleOperator) curOperator;
      List<Source> sources = multipleOperator.getSources();
      for (Source source : sources) {
        isCorrectRoad =
            getRenameOperator(((OperatorSource) source).getOperator(), target, renameOperatorList);
        if (isCorrectRoad) {
          break;
        }
      }
    }

    if (!isCorrectRoad && curOperator.getType() == OperatorType.Rename) {
      renameOperatorList.remove(renameOperatorList.size() - 1);
    }
    return isCorrectRoad;
  }

  private Filter replacePathByRenameMap(Filter filter, Map<String, String> renameMap) {
    switch (filter.getType()) {
      case Or:
        List<Filter> orChildren = ((OrFilter) filter).getChildren();
        for (Filter orChild : orChildren) {
          Filter newFilter = replacePathByRenameMap(orChild, renameMap);
          orChildren.set(orChildren.indexOf(orChild), newFilter);
        }
        break;
      case And:
        List<Filter> andChildren = ((AndFilter) filter).getChildren();
        for (Filter andChild : andChildren) {
          Filter newFilter = replacePathByRenameMap(andChild, renameMap);
          andChildren.set(andChildren.indexOf(andChild), newFilter);
        }
        break;
      case Value:
        String path = ((ValueFilter) filter).getPath();
        for (Map.Entry<String, String> entry : renameMap.entrySet()) {
          path = replacePathByRenameEntry(path, entry);
        }
        return new ValueFilter(
            path, ((ValueFilter) filter).getOp(), ((ValueFilter) filter).getValue());
      case Path:
        String pathA = ((PathFilter) filter).getPathA();
        String pathB = ((PathFilter) filter).getPathB();

        for (Map.Entry<String, String> entry : renameMap.entrySet()) {
          pathA = replacePathByRenameEntry(pathA, entry);
        }

        return new PathFilter(pathA, ((PathFilter) filter).getOp(), pathB);
      default:
        return filter;
    }
    return filter;
  }

  private String replacePathByRenameEntry(String path, Map.Entry<String, String> entry) {
    String nameBeforeRename = entry.getKey();
    String nameAfterRename = entry.getValue();
    if (path.equals(nameAfterRename)) {
      return nameBeforeRename;
    }
    if (path.contains(nameAfterRename)) {
      // 如果path中部分包含nameAfterRename,那么它作为函数参数出现，前面一定有左括号、逗号或者空格，后面一定有右括号或者逗号。
      Pattern pattern = Pattern.compile("[\\s(,](" + nameAfterRename + ")[),]");
      Matcher matcher = pattern.matcher(path);
      StringBuffer sb = new StringBuffer();
      while (matcher.find()) {
        String group = matcher.group();
        if (!group.isEmpty()) {
          matcher.appendReplacement(sb, group.replace(nameAfterRename, nameBeforeRename));
        }
      }
      matcher.appendTail(sb);
      return sb.toString();
    }

    return path;
  }
}
