/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.logical.optimizer.rules;

import cn.edu.tsinghua.iginx.engine.shared.Constants;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.logical.optimizer.core.RuleCall;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.*;
import java.util.stream.IntStream;

public class FilterPushDownSetOpRule extends Rule {

  static final Set<Class> validSetOps =
      new HashSet<>(Arrays.asList(Union.class, Intersect.class, Except.class));

  // 部分情况下Select只能超集下推，这时候需要记录该Select有没有超集下推过，如果有，下次就不能再下推了
  static final Map<AbstractBinaryOperator, Set<Select>> selectMap = new HashMap<>();

  private static class InstanceHolder {
    private static final FilterPushDownSetOpRule INSTANCE = new FilterPushDownSetOpRule();
  }

  public static FilterPushDownSetOpRule getInstance() {
    return InstanceHolder.INSTANCE;
  }

  protected FilterPushDownSetOpRule() {
    /*
     * we want to match the topology like:
     *         Select
     *           |
     *    Union/Intersect/Except
     */
    super(
        "FilterPushDownSetOpRule",
        operand(Select.class, operand(AbstractBinaryOperator.class, any(), any())));
  }

  @Override
  public boolean matches(RuleCall call) {
    // 如果节点是SetOp, 且该Select没有被下推过（即不在selectMap中），那么可以进行下推
    Select select = (Select) call.getMatchedRoot();
    AbstractBinaryOperator operator =
        (AbstractBinaryOperator) ((OperatorSource) select.getSource()).getOperator();
    if (!selectMap.containsKey(operator)) {
      selectMap.put(operator, new HashSet<>());
    }
    return validSetOps.contains(operator.getClass()) && !selectMap.get(operator).contains(select);
  }

  @Override
  public void onMatch(RuleCall call) {
    Select select = (Select) call.getMatchedRoot();
    AbstractBinaryOperator setOp =
        (AbstractBinaryOperator) ((OperatorSource) select.getSource()).getOperator();
    Pair<List<String>, List<String>> order = getSetOpOrder(setOp);

    if (order.k.size() == order.v.size()
        && IntStream.range(0, order.k.size())
            .allMatch(i -> order.k.get(i).equals(order.v.get(i)))) {
      // 如果左右输入的Order相同，那么直接将Filter下推到SetOp的两侧
      Select leftSelect =
          new Select(setOp.getSourceA(), select.getFilter().copy(), select.getTagFilter().copy());
      Select rightSelect =
          new Select(setOp.getSourceB(), select.getFilter().copy(), select.getTagFilter().copy());
      setOp.setSourceA(new OperatorSource(leftSelect));
      setOp.setSourceB(new OperatorSource(rightSelect));
      call.transformTo(setOp);
    } else if (order.k.size() == order.v.size()
        && order.k.stream().noneMatch(path -> path.contains(Constants.ALL_PATH))
        && order.v.stream().noneMatch(path -> path.contains(Constants.ALL_PATH))) {
      // 如果不相同，但是左右两侧的order长度相同且不包含通配符*，那可以将Filter下推到左侧，然后将Filter经过order转换后下推到右侧
      Map<String, String> pathMap = new HashMap<>();
      IntStream.range(0, order.k.size()).forEach(i -> pathMap.put(order.k.get(i), order.v.get(i)));

      Select leftSelect =
          new Select(setOp.getSourceA(), select.getFilter().copy(), select.getTagFilter().copy());
      Select rightSelect =
          new Select(
              setOp.getSourceB(),
              replaceFilterPath(select.getFilter().copy(), pathMap),
              select.getTagFilter().copy());
      setOp.setSourceA(new OperatorSource(leftSelect));
      setOp.setSourceB(new OperatorSource(rightSelect));
      call.transformTo(setOp);
    } else {
      // 如果左右两侧order不同且也带有通配符*，那么无法将filter下推到右侧，只能将filter下推到左侧，并且filter要保留一个在原位置
      Select leftSelect =
          new Select(setOp.getSourceA(), select.getFilter().copy(), select.getTagFilter().copy());
      setOp.setSourceA(new OperatorSource(leftSelect));
      selectMap.get(setOp).add(select);
      call.transformTo(setOp);
    }
  }

  /**
   * 获取SetOp的左右输入的Order
   *
   * @param operator SetOp
   * @return Pair<左输入的Order, 右输入的Order>
   */
  private Pair<List<String>, List<String>> getSetOpOrder(AbstractBinaryOperator operator) {
    if (operator instanceof Union) {
      return new Pair<>(((Union) operator).getLeftOrder(), ((Union) operator).getRightOrder());
    } else if (operator instanceof Intersect) {
      return new Pair<>(
          ((Intersect) operator).getLeftOrder(), ((Intersect) operator).getRightOrder());
    } else if (operator instanceof Except) {
      return new Pair<>(((Except) operator).getLeftOrder(), ((Except) operator).getRightOrder());
    } else {
      return null;
    }
  }

  private Filter replaceFilterPath(Filter filter, Map<String, String> pathMap) {
    filter.accept(
        new FilterVisitor() {
          @Override
          public void visit(AndFilter filter) {}

          @Override
          public void visit(OrFilter filter) {}

          @Override
          public void visit(NotFilter filter) {}

          @Override
          public void visit(KeyFilter filter) {}

          @Override
          public void visit(ValueFilter filter) {
            filter.setPath(pathMap.getOrDefault(pathMap.get(filter.getPath()), filter.getPath()));
          }

          @Override
          public void visit(PathFilter filter) {
            filter.setPathA(
                pathMap.getOrDefault(pathMap.get(filter.getPathA()), filter.getPathA()));
            filter.setPathB(
                pathMap.getOrDefault(pathMap.get(filter.getPathB()), filter.getPathB()));
          }

          @Override
          public void visit(BoolFilter filter) {}

          @Override
          public void visit(ExprFilter filter) {}
        });
    return filter;
  }
}
