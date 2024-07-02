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

import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import cn.edu.tsinghua.iginx.logical.optimizer.core.RuleCall;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FragmentPruningByPatternRule extends Rule {
  /*
     该规则是根据Project的Pattern来判断是否需要Fragment，与列裁剪规则有关，
     列裁剪规则裁剪了Project-Fragment中不需要的列，可能导致该Fragment不再需要
  */

  private static final class InstanceHolder {
    private static final FragmentPruningByPatternRule instance = new FragmentPruningByPatternRule();
  }

  public static FragmentPruningByPatternRule getInstance() {
    return InstanceHolder.instance;
  }

  protected FragmentPruningByPatternRule() {
    /*
     * we want to match the topology like:
     *          Project
     *              |
     *          Fragment
     */
    // Fragment的检测在matches中进行
    super("FragmentPruningByPatternRule", operand(Project.class));
  }

  @Override
  public boolean matches(RuleCall call) {
    Project project = (Project) call.getMatchedRoot();
    if (!(project.getSource() instanceof FragmentSource)) {
      return false;
    }

    return getValidPatterns(project).size() != project.getPatterns().size()
        || project.getPatterns().size() == 0;
  }

  @Override
  public void onMatch(RuleCall call) {
    Project project = (Project) call.getMatchedRoot();
    List<String> validPatterns = getValidPatterns(project);
    project.setPatterns(validPatterns);

    if (validPatterns.size() == 0) {
      PruningFragment(call);
    }
  }

  private static List<String> getValidPatterns(Project project) {
    FragmentSource fragmentSource = (FragmentSource) project.getSource();

    List<String> patterns = project.getPatterns();

    ColumnsInterval columnsInterval = fragmentSource.getFragment().getColumnsInterval();
    List<String> validPatterns = new ArrayList<>();
    for (String pattern : patterns) {
      if (columnsInterval.isContainWithoutPrefix(pattern)) {
        validPatterns.add(pattern);
      }
    }
    return validPatterns;
  }

  /**
   * 从查询树中删除Fragment 向上寻找到Binary节点（如Join、Union等），删除这一侧的分支
   *
   * @param call RuleCall上下文
   */
  private void PruningFragment(RuleCall call) {
    Map<Operator, Operator> parentIndexMap = call.getParentIndexMap();
    Operator curOp = parentIndexMap.get(call.getMatchedRoot());
    Operator lastOp = call.getMatchedRoot();
    if (curOp == null) {
      return;
    }
    while (!OperatorType.isBinaryOperator(curOp.getType())) {
      lastOp = curOp;
      curOp = parentIndexMap.get(curOp);
    }

    BinaryOperator binaryOperator = (BinaryOperator) curOp;
    Source savedSource;
    if (((OperatorSource) binaryOperator.getSourceA()).getOperator() == lastOp) {
      savedSource = binaryOperator.getSourceB();
    } else {
      savedSource = binaryOperator.getSourceA();
    }

    Operator parent = parentIndexMap.get(curOp);

    if (parent != null) {
      if (OperatorType.isUnaryOperator(parent.getType())) {
        ((UnaryOperator) parent).setSource(savedSource);
      } else if (OperatorType.isBinaryOperator(parent.getType())) {
        if (((OperatorSource) ((BinaryOperator) parent).getSourceA()).getOperator() == curOp) {
          ((BinaryOperator) parent).setSourceA(savedSource);
        } else {
          ((BinaryOperator) parent).setSourceB(savedSource);
        }
      }
    }
  }
}
