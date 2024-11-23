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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
/// *
// * IGinX - the polystore system with high performance
// * Copyright (C) Tsinghua University
// *
// * This program is free software: you can redistribute it and/or modify
// * it under the terms of the GNU General Public License as published by
// * the Free Software Foundation, either version 3 of the License, or
// * (at your option) any later version.
// *
// * This program is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU General Public License for more details.
// *
// * You should have received a copy of the GNU General Public License
// * along with this program.  If not, see <http://www.gnu.org/licenses/>.
// */
// package cn.edu.tsinghua.iginx.engine.physical.task.memory.parallel;
//
// import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
// import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.UnaryExecutorFactory;
// import
// cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.stateless.StatelessUnaryExecutor;
// import cn.edu.tsinghua.iginx.engine.physical.task.memory.PipelineMemoryPhysicalTask;
// import cn.edu.tsinghua.iginx.engine.physical.task.memory.UnaryMemoryPhysicalTask;
// import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
// import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchStream;
// import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
// import org.apache.arrow.util.Preconditions;
//
// import java.util.List;
// import java.util.Objects;
//
// public class ParallelPipelineMemoryPhysicalTask
//    extends UnaryMemoryPhysicalTask<BatchStream, BatchStream> {
//
//  private final int parallelism;
//  private final UnaryExecutorFactory<? extends StatelessUnaryExecutor> executorFactory;
//
//  public static ParallelPipelineMemoryPhysicalTask ofRecursive(
//      PipelineMemoryPhysicalTask parentTask,
//      RequestContext context,
//      int parallelism) {
//    return new ParallelPipelineMemoryPhysicalTask(
//        parentTask,
//        parentTask.getOperators(),
//        context,
//        parallelism);
//  }
//
//  public ParallelPipelineMemoryPhysicalTask(
//      PipelineMemoryPhysicalTask parentTask,
//      List<Operator> operators,
//      RequestContext context,
//      int parallelism) {
//    super(parentTask, operators, context);
//    // TODO：递归深入 PipelineMemoryPhysicalTask 直到流水线末端
//    Preconditions.checkArgument(parallelism > 0);
//    this.parallelism = parallelism;
//    this.executorFactory = Objects.requireNonNull(executorFactory);
//  }
//
//  @Override
//  protected BatchStream compute(BatchStream previous) throws PhysicalException {
//    // TODO: 重新生成一组任务，每个 PipelineMemoryPhysicalTask 分成并行份数量的任务，要求可以退化到单线程，防止死锁。
//    // ToDO: 加入一类 sourceMemoryPhysicalTask 用来处理内存临时数据
//    // TODO: 使用 PhysicalEngine.execute() 提交任务
//    // TODO: 原来的 execute 借助 PhysicalEngine.execute() 实现一个 default 方法，以保持兼容性
//  }
//
//  @Override
//  public Class<BatchStream> getResultClass() {
//    return null;
//  }
// }
