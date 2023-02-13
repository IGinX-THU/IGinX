/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package cn.edu.tsinghua.iginx.engine.physical.optimizer.naive;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.physical.optimizer.PhysicalOptimizer;
import cn.edu.tsinghua.iginx.engine.physical.optimizer.ReplicaDispatcher;
import cn.edu.tsinghua.iginx.engine.physical.optimizer.rule.Rule;
import cn.edu.tsinghua.iginx.engine.physical.task.*;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.constraint.ConstraintManager;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import cn.edu.tsinghua.iginx.engine.shared.source.SourceType;
import cn.edu.tsinghua.iginx.sharedstore.utils.RowStreamStoreUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class NaivePhysicalOptimizer implements PhysicalOptimizer {

    private static final Logger logger = LoggerFactory.getLogger(NaivePhysicalOptimizer.class);

    public static NaivePhysicalOptimizer getInstance() {
        return NaivePhysicalOptimizerHolder.INSTANCE;
    }

    @Override
    public PhysicalTask optimize(RequestContext context, Operator root) {
        if (root == null) {
            return null;
        }
        if (context.isEnableFaultTolerance()) {
            markSequence(context, root, 1);
            if (context.isRecover()) {
                root = trimOperatorTreeBySharedStorage(context, root);
            }
        }
        return constructTask(context, root);
    }

    // 改写操作符树，如果操作符输出结果已经被缓存了，则用 Load 节点替换以该节点为跟的子树
    private Operator trimOperatorTreeBySharedStorage(RequestContext ctx, Operator root) {
        String key = RowStreamStoreUtils.encodeKey(ctx.getId(), root.getSequence());
        if (RowStreamStoreUtils.checkRowStream(key)) {
            return new Load(key);
        }
        if (OperatorType.isUnaryOperator(root.getType())) {
            Source source = ((UnaryOperator) root).getSource();
            if (source.getType() == SourceType.Fragment) {
                return root;
            }
            OperatorSource operatorSource = (OperatorSource) source;
            operatorSource.setOperator(trimOperatorTreeBySharedStorage(ctx, operatorSource.getOperator()));
        } else if (OperatorType.isBinaryOperator(root.getType())) {
            BinaryOperator binaryOperator = (BinaryOperator) root;
            OperatorSource sourceA = (OperatorSource) binaryOperator.getSourceA();
            OperatorSource sourceB = (OperatorSource) binaryOperator.getSourceB();
            sourceA.setOperator(trimOperatorTreeBySharedStorage(ctx, sourceA.getOperator()));
            sourceB.setOperator(trimOperatorTreeBySharedStorage(ctx, sourceB.getOperator()));
        } else {
            MultipleOperator multipleOperator = (MultipleOperator) root;
            List<Source> sources = multipleOperator.getSources();
            for (Source source : sources) {
                OperatorSource operatorSource = (OperatorSource) source;
                operatorSource.setOperator(trimOperatorTreeBySharedStorage(ctx, operatorSource.getOperator()));
            }
        }
        return root;
    }

    private int markSequence(RequestContext ctx, Operator operator, int sequence) { // sequence 为标记当前节点的编号
        operator.setSequence(sequence);
        logger.info("[LongQuery][NaivePhysicalOptimizer][queryId={}] mark operator [{}] as {}", ctx.getId(), operator.getInfo(), sequence);
        sequence += 1;
        if (OperatorType.isUnaryOperator(operator.getType())) {
            UnaryOperator unaryOperator = (UnaryOperator) operator;
            Source source = unaryOperator.getSource();
            if (source.getType() != SourceType.Fragment) { // 构建物理计划
                OperatorSource operatorSource = (OperatorSource) source;
                sequence =  markSequence(ctx, operatorSource.getOperator(), sequence);
            }
        } else if (OperatorType.isBinaryOperator(operator.getType())) {
            BinaryOperator binaryOperator = (BinaryOperator) operator;
            OperatorSource sourceA = (OperatorSource) binaryOperator.getSourceA();
            OperatorSource sourceB = (OperatorSource) binaryOperator.getSourceB();
            sequence = markSequence(ctx, sourceA.getOperator(), sequence);
            sequence = markSequence(ctx, sourceB.getOperator(), sequence);
        } else {
            MultipleOperator multipleOperator = (MultipleOperator) operator;
            List<Source> sources = multipleOperator.getSources();
            for (Source source : sources) {
                OperatorSource operatorSource = (OperatorSource) source;
                sequence = markSequence(ctx, operatorSource.getOperator(), sequence);
            }
        }
        return sequence;
    }

    @Override
    public ConstraintManager getConstraintManager() {
        return NaiveConstraintManager.getInstance();
    }

    @Override
    public ReplicaDispatcher getReplicaDispatcher() {
        return NaiveReplicaDispatcher.getInstance();
    }

    @Override
    public void setRules(Collection<Rule> rules) {

    }

    // 自顶向下构建计划
    private PhysicalTask constructTask(RequestContext ctx, Operator operator) {
        if (OperatorType.isUnaryOperator(operator.getType())) {
            UnaryOperator unaryOperator = (UnaryOperator) operator;
            Source source = unaryOperator.getSource();
            if (source.getType() == SourceType.Fragment) { // 构建物理计划
                List<Operator> operators = new ArrayList<>();
                operators.add(operator);
                if (OperatorType.isNeedBroadcasting(operator.getType())) {
                    return new StoragePhysicalTask(operators, ctx, true, true);
                } else {
                    return new StoragePhysicalTask(operators, ctx);
                }
            } else if (source.getType() == SourceType.SharedStore) {
                // 当前的操作符必定为 load，此类任务虽然是内存任务，但是没有父节点
                List<Operator> operators = new ArrayList<>();
                operators.add(operator);
                return new UnaryMemoryPhysicalTask(operators, ctx, null);
            } else { // 构建内存中的计划
                OperatorSource operatorSource = (OperatorSource) source;
//                Operator sourceOperator = operatorSource.getOperator();
                PhysicalTask sourceTask = constructTask(ctx, operatorSource.getOperator());
//                if (ConfigDescriptor.getInstance().getConfig().isEnablePushDown() && sourceTask instanceof StoragePhysicalTask
//                        && sourceOperator.getType() == OperatorType.Project
//                        && ((Project) sourceOperator).getTagFilter() == null
//                        && ((UnaryOperator) sourceOperator).getSource().getType() == SourceType.Fragment
//                    && operator.getType() == OperatorType.Select && ((Select) operator).getTagFilter() == null) {
//                    sourceTask.getOperators().add(operator);
//                    return sourceTask;
//                }
                List<Operator> operators = new ArrayList<>();
                operators.add(operator);
                PhysicalTask task = new UnaryMemoryPhysicalTask(operators, ctx, sourceTask);
                sourceTask.setFollowerTask(task);
                return task;
            }
        } else if (OperatorType.isBinaryOperator(operator.getType())) {
            BinaryOperator binaryOperator = (BinaryOperator) operator;
            OperatorSource sourceA = (OperatorSource) binaryOperator.getSourceA();
            OperatorSource sourceB = (OperatorSource) binaryOperator.getSourceB();
            PhysicalTask sourceTaskA = constructTask(ctx, sourceA.getOperator());
            PhysicalTask sourceTaskB = constructTask(ctx, sourceB.getOperator());
            List<Operator> operators = new ArrayList<>();
            operators.add(operator);
            PhysicalTask task = new BinaryMemoryPhysicalTask(operators, ctx, sourceTaskA, sourceTaskB);
            sourceTaskA.setFollowerTask(task);
            sourceTaskB.setFollowerTask(task);
            return task;
        } else {
            MultipleOperator multipleOperator = (MultipleOperator) operator;
            List<Source> sources = multipleOperator.getSources();
            List<PhysicalTask> parentTasks = new ArrayList<>();
            for (Source source : sources) {
                OperatorSource operatorSource = (OperatorSource) source;
                PhysicalTask parentTask = constructTask(ctx, operatorSource.getOperator());
                parentTasks.add(parentTask);
            }
            List<Operator> operators = new ArrayList<>();
            operators.add(operator);
            PhysicalTask task = new MultipleMemoryPhysicalTask(operators, ctx, parentTasks);
            for (PhysicalTask parentTask : parentTasks) {
                parentTask.setFollowerTask(task);
            }
            return task;
        }
    }

    private static class NaivePhysicalOptimizerHolder {

        private static final NaivePhysicalOptimizer INSTANCE = new NaivePhysicalOptimizer();

        private NaivePhysicalOptimizerHolder() {
        }

    }

}
