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
package cn.edu.tsinghua.iginx.physical.optimizer.naive;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
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
      UnaryOperator unaryOperator = (UnaryOperator) operator;
      Source source = unaryOperator.getSource();
      if (source.getType() == SourceType.Fragment) { // 构建物理计划
        List<Operator> operators = new ArrayList<>();
        operators.add(operator);
        if (OperatorType.isNeedBroadcasting(operator.getType())) {
          return new StoragePhysicalTask(operators, true, true, context);
        } else {
          return new StoragePhysicalTask(operators, context);
        }
      } else { // 构建内存中的计划
        OperatorSource operatorSource = (OperatorSource) source;
        Operator sourceOperator = operatorSource.getOperator();
        PhysicalTask sourceTask = constructTask(operatorSource.getOperator(), context);
        if (ConfigDescriptor.getInstance().getConfig().isEnablePushDown()
            && sourceTask instanceof StoragePhysicalTask
            && sourceOperator.getType() == OperatorType.Project
            && ((Project) sourceOperator).getTagFilter() == null
            && ((UnaryOperator) sourceOperator).getSource().getType() == SourceType.Fragment
            && operator.getType() == OperatorType.Select
            && ((Select) operator).getTagFilter() == null) {
          sourceTask.getOperators().add(operator);
          return sourceTask;
        }
        List<Operator> operators = new ArrayList<>();
        operators.add(operator);
        PhysicalTask task = new UnaryMemoryPhysicalTask(operators, sourceTask, context);
        sourceTask.setFollowerTask(task);
        return task;
      }
    } else if (OperatorType.isBinaryOperator(operator.getType())) {
      BinaryOperator binaryOperator = (BinaryOperator) operator;
      OperatorSource sourceA = (OperatorSource) binaryOperator.getSourceA();
      OperatorSource sourceB = (OperatorSource) binaryOperator.getSourceB();
      PhysicalTask sourceTaskA = constructTask(sourceA.getOperator(), context);
      PhysicalTask sourceTaskB = constructTask(sourceB.getOperator(), context);
      List<Operator> operators = new ArrayList<>();
      operators.add(operator);
      PhysicalTask task =
          new BinaryMemoryPhysicalTask(operators, sourceTaskA, sourceTaskB, context);
      sourceTaskA.setFollowerTask(task);
      sourceTaskB.setFollowerTask(task);
      return task;
    } else if (operator.getType().equals(OperatorType.ShowColumns)) {
      return new GlobalPhysicalTask(operator, context);
    } else {
      MultipleOperator multipleOperator = (MultipleOperator) operator;
      List<Source> sources = multipleOperator.getSources();
      List<PhysicalTask> parentTasks = new ArrayList<>();
      for (Source source : sources) {
        OperatorSource operatorSource = (OperatorSource) source;
        PhysicalTask parentTask = constructTask(operatorSource.getOperator(), context);
        parentTasks.add(parentTask);
      }
      List<Operator> operators = new ArrayList<>();
      operators.add(operator);

      PhysicalTask task;
      if (operator.getType().equals(OperatorType.Folded)) {
        FoldedOperator foldedOperator = (FoldedOperator) multipleOperator;
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
  }

  private static class NaivePhysicalOptimizerHolder {

    private static final NaivePhysicalOptimizer INSTANCE = new NaivePhysicalOptimizer();

    private NaivePhysicalOptimizerHolder() {}
  }
}
