/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.physical.optimizer.naive;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.logical.utils.OperatorUtils;
import cn.edu.tsinghua.iginx.engine.physical.memory.MemoryPhysicalTaskDispatcher;
import cn.edu.tsinghua.iginx.engine.physical.optimizer.PhysicalOptimizer;
import cn.edu.tsinghua.iginx.engine.physical.optimizer.ReplicaDispatcher;
import cn.edu.tsinghua.iginx.engine.physical.task.*;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.constraint.ConstraintManager;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import cn.edu.tsinghua.iginx.engine.shared.source.SourceType;
import cn.edu.tsinghua.iginx.physical.optimizer.rule.Rule;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class NaivePhysicalOptimizer implements PhysicalOptimizer {

  public static NaivePhysicalOptimizer getInstance() {
    return NaivePhysicalOptimizerHolder.INSTANCE;
  }

  @Override
  public PhysicalTask optimize(Operator root, RequestContext context) {
    if (root == null) {
      return null;
    }
    return constructTask(root, context);
  }

  @Override
  public ConstraintManager getConstraintManager() {
    return NaiveConstraintManager.getInstance();
  }

  @Override
  public ReplicaDispatcher getReplicaDispatcher() {
    return NaiveReplicaDispatcher.getInstance();
  }

  public void setRules(Collection<Rule> rules) {}

  private PhysicalTask constructTask(Operator operator, RequestContext context) {
    if (OperatorType.isUnaryOperator(operator.getType())) {
      return constructUnaryTask((UnaryOperator) operator, context);
    } else if (OperatorType.isBinaryOperator(operator.getType())) {
      return constructBinaryTask((BinaryOperator) operator, context);
    } else if (operator.getType().equals(OperatorType.ShowColumns)) {
      return new GlobalPhysicalTask(operator, context);
    } else if (OperatorType.isMultipleOperator(operator.getType())) {
      return constructMultipleTask((MultipleOperator) operator, context);
    } else {
      throw new IllegalArgumentException("Unexpected operator type: " + operator.getType());
    }
  }

  private PhysicalTask constructUnaryTask(UnaryOperator operator, RequestContext context) {
    List<Operator> operators = new ArrayList<>();
    Source source = operator.getSource();
    switch (source.getType()) {
      case Fragment: // 构建物理计划
        operators.add(operator);
        if (OperatorType.isNeedBroadcasting(operator.getType())) {
          return new StoragePhysicalTask(operators, true, true, context);
        } else {
          return new StoragePhysicalTask(operators, context);
        }
      case Operator: // 构建内存中的计划
        OperatorSource operatorSource = (OperatorSource) source;
        Operator sourceOperator = operatorSource.getOperator();
        PhysicalTask sourceTask = constructTask(operatorSource.getOperator(), context);
        // push down Select operator
        if (ConfigDescriptor.getInstance().getConfig().isEnablePushDown()
            && sourceTask instanceof StoragePhysicalTask) {
          if (sourceOperator.getType() == OperatorType.Project
              && ((Project) sourceOperator).getTagFilter() == null
              && ((UnaryOperator) sourceOperator).getSource().getType() == SourceType.Fragment) {
            switch (operator.getType()) {
              case Select:
                if (((Select) operator).getTagFilter() == null) {
                  sourceTask.getOperators().add(operator);
                  return sourceTask;
                }
                break;
              case SetTransform:
              case GroupBy:
                sourceTask.getOperators().add(operator);
                return sourceTask;
              default:
                break;
            }
          } else if ((operator.getType() == OperatorType.GroupBy
                  || operator.getType() == OperatorType.SetTransform)
              && sourceOperator.getType() == OperatorType.Select
              && OperatorUtils.getUnaryChild(sourceOperator) != null
              && OperatorUtils.getUnaryChild(sourceOperator).getType() == OperatorType.Project
              && ((UnaryOperator) OperatorUtils.getUnaryChild(sourceOperator)).getSource().getType()
                  == SourceType.Fragment) {
            sourceTask.getOperators().add(operator);
            return sourceTask;
          }
        }
        operators.add(operator);
        PhysicalTask task = new UnaryMemoryPhysicalTask(operators, sourceTask, context);
        sourceTask.setFollowerTask(task);
        return task;
      case Constant:
        operators.add(operator);
        UnaryMemoryPhysicalTask ret = new UnaryMemoryPhysicalTask(operators, null, context);
        MemoryPhysicalTaskDispatcher.getInstance().addMemoryTask(ret);
        return ret;
      default:
        throw new IllegalArgumentException("Unsupported operator type: " + source.getType());
    }
  }

  private PhysicalTask constructBinaryTask(BinaryOperator operator, RequestContext context) {
    OperatorSource sourceA = (OperatorSource) operator.getSourceA();
    OperatorSource sourceB = (OperatorSource) operator.getSourceB();
    PhysicalTask sourceTaskA = constructTask(sourceA.getOperator(), context);
    PhysicalTask sourceTaskB = constructTask(sourceB.getOperator(), context);
    List<Operator> operators = new ArrayList<>();
    operators.add(operator);
    PhysicalTask task = new BinaryMemoryPhysicalTask(operators, sourceTaskA, sourceTaskB, context);
    sourceTaskA.setFollowerTask(task);
    sourceTaskB.setFollowerTask(task);
    return task;
  }

  private PhysicalTask constructMultipleTask(MultipleOperator operator, RequestContext context) {
    List<PhysicalTask> parentTasks = new ArrayList<>();
    for (Source source : operator.getSources()) {
      OperatorSource operatorSource = (OperatorSource) source;
      PhysicalTask parentTask = constructTask(operatorSource.getOperator(), context);
      parentTasks.add(parentTask);
    }
    List<Operator> operators = new ArrayList<>();
    operators.add(operator);

    PhysicalTask task;
    if (operator.getType().equals(OperatorType.Folded)) {
      FoldedOperator foldedOperator = (FoldedOperator) operator;
      PhysicalTask foldedTask =
          new FoldedMemoryPhysicalTask(
              operators, foldedOperator.getIncompleteRoot(), parentTasks, context);
      for (PhysicalTask parentTask : parentTasks) {
        parentTask.setFollowerTask(foldedTask);
      }
      task = new CompletedFoldedPhysicalTask(foldedTask, context);
      foldedTask.setFollowerTask(task);
    } else {
      task = new MultipleMemoryPhysicalTask(operators, parentTasks, context);
      for (PhysicalTask parentTask : parentTasks) {
        parentTask.setFollowerTask(task);
      }
    }
    return task;
  }

  private static class NaivePhysicalOptimizerHolder {

    private static final NaivePhysicalOptimizer INSTANCE = new NaivePhysicalOptimizer();

    private NaivePhysicalOptimizerHolder() {}
  }
}
