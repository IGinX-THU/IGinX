package cn.edu.tsinghua.iginx.engine.logical.optimizer.rules;

import cn.edu.tsinghua.iginx.engine.logical.optimizer.core.RuleCall;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.ExprUtils;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils;
import cn.edu.tsinghua.iginx.engine.shared.Constants;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionCall;
import cn.edu.tsinghua.iginx.engine.shared.function.system.ArithmeticExpr;
import cn.edu.tsinghua.iginx.engine.shared.function.system.First;
import cn.edu.tsinghua.iginx.engine.shared.function.system.Last;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.AndTagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import java.util.*;
import java.util.logging.Logger;

public class ColumnPruningRule extends Rule {

  private static final Logger LOGGER = Logger.getLogger(ColumnPruningRule.class.getName());

  private static final class InstanceHolder {
    static final ColumnPruningRule INSTANCE = new ColumnPruningRule();
  }

  public static ColumnPruningRule getInstance() {
    return InstanceHolder.INSTANCE;
  }

  protected ColumnPruningRule() {
    /*
     * we want to match the topology like:
     *         Any
     */
    // 匹配任意操作符，但由于策略是ONCE，所以只会匹配到树的顶端
    super("ColumnPruningRule", operand(AbstractOperator.class, any()), RuleStrategy.ONCE);
  }

  @Override
  public boolean matches(RuleCall call) {
    return true;
  }

  @Override
  public void onMatch(RuleCall call) {
    /*
     大致优化思路是，对于Project操作符，向上查找所有父节点，统计父节点所需的列，然后将Project操作符中不需要的列去掉
    */
    collectColumns(call.getMatchedRoot(), new HashSet<>(), null, new ArrayList<>());
  }

  private void collectColumns(
      Operator operator,
      Set<String> columns,
      TagFilter tagFilter,
      List<Operator> visitedOperators) {
    if (OperatorType.isUnaryOperator(operator.getType())) {
      List<String> newColumnList = null;
      List<FunctionCall> functionCallList = null;

      if (operator.getType() == OperatorType.Select) {
        // 提取filter中的列
        Select select = (Select) operator;
        Filter filter = select.getFilter();
        newColumnList = FilterUtils.getAllPathsFromFilter(filter);
      } else if (operator.getType() == OperatorType.Reorder) {
        Reorder reorder = (Reorder) operator;
        if (columns.isEmpty()) {
          columns.addAll(reorder.getPatterns());
        } else if (hasWildCard(columns)) {
          if (checkCoverage(reorder.getPatterns(), columns)){

          }
        } else {
          ArrayList<String> patterns = new ArrayList<>(columns);
          Collections.sort(patterns);
          reorder.setPatterns(patterns);
        }
      } else if (operator.getType() == OperatorType.Project) {
        Project project = (Project) operator;
        if (columns.isEmpty()) {
          columns.addAll(project.getPatterns());
          tagFilter = project.getTagFilter();
        } else if (hasWildCard(columns)) {
          // 要考虑cover情况
          columns.clear();
          columns.addAll(project.getPatterns());
          tagFilter = project.getTagFilter();
        } else {
          ArrayList<String> patterns = new ArrayList<>(columns);
          Collections.sort(patterns);
          project.setPatterns(patterns);
          if (tagFilter == null) {
            tagFilter = project.getTagFilter();
          } else if (!Objects.equals(tagFilter.toString(), project.getTagFilter().toString())) {
            tagFilter = new AndTagFilter(Arrays.asList(tagFilter, project.getTagFilter()));
          }
        }
      } else if (operator.getType() == OperatorType.Rename) {
        Rename rename = (Rename) operator;
        Map<String, String> aliasMap = rename.getAliasMap();
        columns = new HashSet<>(recoverRenamedPatterns(aliasMap, new ArrayList<>(columns)));

      } else if (operator.getType() == OperatorType.GroupBy) {
        GroupBy groupBy = (GroupBy) operator;
        newColumnList = groupBy.getGroupByCols();
        functionCallList = groupBy.getFunctionCallList();
      } else if (operator.getType() == OperatorType.Downsample) {
        Downsample downsample = (Downsample) operator;
        functionCallList = downsample.getFunctionCallList();
      } else if (operator.getType() == OperatorType.Sort) {
        Sort sort = (Sort) operator;
        newColumnList = sort.getSortByCols();
      } else if (operator.getType() == OperatorType.AddSchemaPrefix) {
        String prefix = ((AddSchemaPrefix) operator).getSchemaPrefix();
        if (prefix != null) {
          Set<String> newColumns = new HashSet<>();
          for (String column : columns) {
            newColumns.add(prefix + "." + column);
          }
          columns = newColumns;
        }
      } else if (operator.getType() == OperatorType.SetTransform) {
        SetTransform setTransform = (SetTransform) operator;
        functionCallList = setTransform.getFunctionCallList();
      } else if (operator.getType() == OperatorType.RowTransform) {
        RowTransform rowTransform = (RowTransform) operator;
        functionCallList = rowTransform.getFunctionCallList();
      } else if (operator.getType() == OperatorType.MappingTransform) {
        MappingTransform mappingTransform = (MappingTransform) operator;
        functionCallList = mappingTransform.getFunctionCallList();
      }

      if (newColumnList != null) {
        // 检测newColumnList中的列是否有被原columns覆盖的列
        Set<String> removeCoveredColumnList = new HashSet<>(); // 去除了被覆盖的列后的列列表
        for (String column : newColumnList) {
          boolean covered = false;
          for (String c : columns) {
            if (covers(c, column)) {
              covered = true;
              break;
            }
          }
          if (!covered) {
            removeCoveredColumnList.add(column);
          }
        }

        // 如果有新列，且该节点的父节点不是Project,
        // 说明我们应该在该节点上方加入一个Project节点，来裁剪掉不需要的列
        if (!visitedOperators.isEmpty() && !removeCoveredColumnList.isEmpty()) {
          Operator parent = visitedOperators.get(visitedOperators.size() - 1);
          if (parent.getType() != OperatorType.Project
              && parent.getType() != OperatorType.Reorder) {
            Project project =
                new Project(new OperatorSource(operator), new ArrayList<>(columns), tagFilter);
            if (OperatorType.isUnaryOperator(parent.getType())) {
              ((UnaryOperator) parent).setSource(new OperatorSource(project));
            } else if (OperatorType.isBinaryOperator(parent.getType())) {
              if (((OperatorSource) ((BinaryOperator) parent).getSourceA()).getOperator()
                  == operator) {
                ((BinaryOperator) parent).setSourceA(new OperatorSource(project));
              } else {
                ((BinaryOperator) parent).setSourceB(new OperatorSource(project));
              }
            }
          }
        }

        columns.addAll(removeCoveredColumnList);
      }
      if (functionCallList != null) {
        changeColumnsFromFunctionCallList(functionCallList, columns);
      }

      // 递归处理下一个节点
      visitedOperators.add(operator);
      if (((UnaryOperator) operator).getSource() instanceof FragmentSource) {
        return;
      }
      collectColumns(
          ((OperatorSource) ((UnaryOperator) operator).getSource()).getOperator(),
          columns,
          tagFilter,
          visitedOperators);

    } else if (OperatorType.isBinaryOperator(operator.getType())) {
      // 下面是处理BinaryOperator的情况，其中只有Join操作符需要处理
      String PrefixA = null, PrefixB = null;
      Filter filter = null;
      boolean isNaturalJoin = false;
      Set<OperatorType> processedTypes =
          new HashSet<>(
              Arrays.asList(
                  OperatorType.InnerJoin,
                  OperatorType.OuterJoin,
                  OperatorType.CrossJoin,
                  OperatorType.MarkJoin));

      if (operator.getType() == OperatorType.InnerJoin) {
        InnerJoin innerJoin = (InnerJoin) operator;
        PrefixA = innerJoin.getPrefixA();
        PrefixB = innerJoin.getPrefixB();
        filter = innerJoin.getFilter();
        isNaturalJoin = innerJoin.isNaturalJoin();
      } else if (operator.getType() == OperatorType.OuterJoin) {
        OuterJoin outerJoin = (OuterJoin) operator;
        PrefixA = outerJoin.getPrefixA();
        PrefixB = outerJoin.getPrefixB();
        filter = outerJoin.getFilter();
        isNaturalJoin = outerJoin.isNaturalJoin();
      } else if (operator.getType() == OperatorType.CrossJoin) {
        CrossJoin crossJoin = (CrossJoin) operator;
        PrefixA = crossJoin.getPrefixA();
        PrefixB = crossJoin.getPrefixB();
      } else if(operator.getType() == OperatorType.SingleJoin){
        // SingleJoin不带有信息，不做处理
        return;
      }
      else if (operator.getType() == OperatorType.MarkJoin) {
        // MarkJoin暂不处理
        return;
      }

      if (isNaturalJoin) {
        // NaturalJoin可能会用到所有的列，因此我们直接返回，不继续优化
        return;
      }

      if (filter != null) {
        columns.addAll(FilterUtils.getAllPathsFromFilter(filter));
      }

      // 将columns中的列名分成以PrefixA和PrefixB开头的两部分
      Set<String> prefixAColumns = new HashSet<>();
      Set<String> prefixBColumns = new HashSet<>();
      if (PrefixA != null && PrefixB != null) {
        for (String column : columns) {
          if (column.contains("*")) {
            /*
            如果现有的列中包含通配符*，这个通配符会受到当前Join算子的限制，不再能下推，再推下去会导致取的列过多
            例如最顶上Project的test.*，但是JOIN算子左右两侧是test.a.*和test.b.*，这是的test.*实际上取的列是test.a.*和test.b.*的并集
            但如果将test.*下推到JOIN算子后，会取到test.a.*和test.b.*之外的列，因此我们丢弃所有列，用Join算子的左右两个表覆盖
            */
            prefixAColumns.clear();
            prefixBColumns.clear();
            prefixAColumns.add(PrefixA + ".*");
            prefixBColumns.add(PrefixB + ".*");
            break;
          }

          if (column.startsWith(PrefixA + ".")) {
            prefixAColumns.add(column);
          } else if (column.startsWith(PrefixB + ".")) {
            prefixBColumns.add(column);
          } else {
            // 如果没有匹配的话，可能是连续JOIN的情况，即SELECT t1.*, t2.*, t3.* FROM t1 JOIN t2 JOIN t3;(这里省略了ON条件）
            // 目前看来第一个InnerJoin只有t1、t2的信息，t3的InnerJoin放在左子树(PrefixA)中
            prefixAColumns.add(column);
          }
        }
      } else if (processedTypes.contains(operator.getType())) {
        // 在没有Prefix的情况下，缺少信息，无法继续优化
        return;
      }

      // 如果不存在Filter，这几个Join算子会执行笛卡尔积，在上面分Columns时可能会出现其中一侧的Columns为空的情况
      // 此时我们将空的一侧的Columns设置为PRIFIX.*
      if (filter == null && PrefixA != null && PrefixB != null) {
        if (prefixAColumns.isEmpty()) {
          prefixAColumns.add(PrefixA + ".*");
        }
        if (prefixBColumns.isEmpty()) {
          prefixBColumns.add(PrefixB + ".*");
        }
      }

      if (prefixAColumns.isEmpty() || prefixBColumns.isEmpty()) {
        prefixAColumns = columns;
        prefixBColumns.addAll(columns);
      }

      // 递归处理下一个节点
      visitedOperators.add(operator);
      collectColumns(
          ((OperatorSource) ((BinaryOperator) operator).getSourceA()).getOperator(),
          prefixAColumns,
          tagFilter,
          visitedOperators);
      collectColumns(
          ((OperatorSource) ((BinaryOperator) operator).getSourceB()).getOperator(),
          prefixBColumns,
          tagFilter,
          new ArrayList<>(visitedOperators));
    }
  }

  /**
   * 从FunctionCallList中将columns中函数结果列还原为参数列
   *
   * @param functionCallList 函数列表
   * @param columns 优化过程中的列列表，我们直接在这个列表上进行修改
   */
  private void changeColumnsFromFunctionCallList(
      List<FunctionCall> functionCallList, Set<String> columns) {
    for (FunctionCall functionCall : functionCallList) {
      String functionName = functionCall.getFunction().getIdentifier();
      List<String> paths = functionCall.getParams().getPaths();

      if (functionName.equals(First.FIRST) || functionName.equals(Last.LAST)) {
        // First和Last的结果列是path和value
        columns.remove("path");
        columns.remove("value");
        columns.addAll(paths);
      } else if (functionName.equals(ArithmeticExpr.ARITHMETIC_EXPR)) {
        String functionStr = functionCall.getParams().getExpr().getColumnName();
        columns.remove(functionStr);
        columns.addAll(ExprUtils.getPathFromExpr(functionCall.getParams().getExpr()));
      } else {
        String functionStr = functionName + "(" + String.join(", ", paths) + ")";
        if (functionCall.getParams().isDistinct()) {
          functionStr = functionName + "(distinct " + String.join(", ", paths) + ")";
        }
        columns.addAll(paths);
        if (columns.contains(functionStr)) {
          columns.remove(functionStr);
        } else {
          LOGGER.warning("FunctionCallList中的函数结果列不在columns中");
        }
      }
    }
  }

  // 判断是否模式a可以覆盖模式b
  private static boolean covers(String a, String b) {
    // 使用.*作为分隔符分割模式
    String[] partsA = a.split("\\*");
    String[] partsB = b.split("\\*");

    int indexB = 0;
    for (String part : partsA) {
      boolean found = false;
      while (indexB < partsB.length) {
        if (partsB[indexB].contains(part)) {
          found = true;
          indexB++; // 移动到下一个部分
          break;
        }
        indexB++;
      }
      if (!found) {
        return false; // 如果任何部分未找到匹配，则模式a不能覆盖模式b
      }
    }
    return true;
  }

  // 检查第一组模式是否完全包含第二组模式
  public static boolean checkCoverage(Collection<String> groupA, Collection<String> groupB) {
    for (String patternB : groupB) {
      boolean covered = false;
      for (String patternA : groupA) {
        if (covers(patternA, patternB)) {
          covered = true;
          break;
        }
      }
      if (!covered) {
        return false; // 如果找不到覆盖patternB的patternA，则第一组不完全包含第二组
      }
    }
    return true; // 所有的patternB都被至少一个patternA覆盖
  }

  /**
   * 反向重命名模式列表中的模式
   *
   * @param aliasMap 重命名规则, key为旧模式，value为新模式，在这里我们要将新模式恢复为旧模式
   * @param patterns 要重命名的模式列表
   * @return 重命名后的模式列表
   */
  private static List<String> recoverRenamedPatterns(
      Map<String, String> aliasMap, List<String> patterns) {
    List<String> renamedPatterns = new ArrayList<>();
    for (String pattern : patterns) {
      boolean matched = false;
      for (Map.Entry<String, String> entry : aliasMap.entrySet()) {
        String oldPattern = entry.getKey().replace("*", "$1"); // 通配符转换为正则的捕获组
        String newPattern = entry.getValue().replace("*", "(.*)"); // 使用反向引用保留原始匹配的部分
        if (pattern.matches(newPattern)) {
          String p = pattern.replaceAll(newPattern, oldPattern);
          renamedPatterns.add(p);
          matched = true;
          break;
        }
      }
      if (!matched) { // 如果没有匹配的规则，添加原始模式
        renamedPatterns.add(pattern);
      }
    }
    return renamedPatterns;
  }

  /**
   * 检查列中是否有通配符
   *
   * @param columns 列列表
   * @return 是否有通配符
   */
  private static boolean hasWildCard(Set<String> columns) {
    for (String column : columns) {
      if (column.contains(Constants.ALL_PATH)) {
        return true;
      }
    }
    return false;
  }


}
