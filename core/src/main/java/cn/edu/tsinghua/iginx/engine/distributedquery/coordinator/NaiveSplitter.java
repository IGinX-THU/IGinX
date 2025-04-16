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
package cn.edu.tsinghua.iginx.engine.distributedquery.coordinator;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.distributedquery.constraint.Constraint;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.visitor.LeafVisitor;
import cn.edu.tsinghua.iginx.engine.shared.source.IGinXSource;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import cn.edu.tsinghua.iginx.engine.shared.source.SourceType;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.IginxMeta;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.*;

public class NaiveSplitter implements Splitter {

  private final IMetaManager metaManager = DefaultMetaManager.getInstance();

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private final String selfIp = config.getIp();
  private final int selfPort = config.getPort();

  private static class NaiveSplitterHolder {
    private static final NaiveSplitter INSTANCE = new NaiveSplitter();
  }

  public static NaiveSplitter getInstance() {
    return NaiveSplitterHolder.INSTANCE;
  }

  @Override
  public Plan split(Operator root) {
    List<IginxMeta> availableIginx = new ArrayList<>(metaManager.getIginxList());
    //    List<IginxMeta> availableIginx = new ArrayList<>();
    //    availableIginx.add(new IginxMeta(1, "127.0.0.1", 1111, null));
    //    availableIginx.add(new IginxMeta(1, "127.0.0.1", 2222, null));
    //    availableIginx.add(new IginxMeta(1, "127.0.0.1", 3333, null));
    //    availableIginx.add(new IginxMeta(1, "0.0.0.0", 6888, null));
    int k = availableIginx.size() - 1;

    Pair<Queue<Operator>, Map<Operator, Operator>> pair = split(root, k);
    Queue<Operator> splitPartQueue = pair.getK();
    Map<Operator, Operator> parents = pair.getV();
    List<Operator> subPlans = new ArrayList<>();

    int index = 0;
    while (!splitPartQueue.isEmpty()) {
      Operator op = splitPartQueue.poll();
      IginxMeta iginxMeta = availableIginx.get(index++);
      if (isSelf(iginxMeta)) {
        iginxMeta = availableIginx.get(index++);
      }
      Load load = new Load(new IGinXSource(iginxMeta.getIp(), iginxMeta.getPort()), index, op);
      subPlans.add(op);

      Operator parent = parents.get(op);
      if (parent instanceof UnaryOperator) {
        ((UnaryOperator) parent).setSource(new OperatorSource(load));
      } else if (parent instanceof BinaryOperator) {
        Operator childA = ((OperatorSource) ((BinaryOperator) parent).getSourceA()).getOperator();
        if (childA.equals(op)) {
          ((BinaryOperator) parent).setSourceA(new OperatorSource(load));
        } else {
          ((BinaryOperator) parent).setSourceB(new OperatorSource(load));
        }
      }
    }

    // return new Plan(root, subPlans);
    return null;
  }

  private Map<IginxMeta, Operator> init(Operator root, List<IginxMeta> nodes) {
    LeafVisitor visitor = new LeafVisitor();
    root.accept(visitor);
    List<Operator> resultOps = visitor.getLeafOps(); // 将算子树的所有叶子节点作为初始的子任务分割集合

    return null;
  }

  List<IginxMeta> allocateNodes(
      List<Operator> resultOps, List<IginxMeta> nodes, List<Constraint> constraints) {
    return null;
  }

  private Pair<Queue<Operator>, Map<Operator, Operator>> split(Operator root, int k) {
    Queue<Operator> queue = new ArrayDeque<>();
    Map<Operator, Operator> parents = new HashMap<>();
    queue.offer(root);
    while (queue.size() < k) {
      Operator op = queue.poll();

      if (op instanceof UnaryOperator) {
        Source source = ((UnaryOperator) op).getSource();
        if (source.getType().equals(SourceType.Fragment)) {
          queue.offer(op);
          continue;
        }

        Operator child = ((OperatorSource) source).getOperator();
        parents.put(child, op);
        queue.offer(child);
      } else if (op instanceof BinaryOperator) {
        Operator childA = ((OperatorSource) ((BinaryOperator) op).getSourceA()).getOperator();
        Operator childB = ((OperatorSource) ((BinaryOperator) op).getSourceB()).getOperator();

        parents.put(childA, op);
        parents.put(childB, op);
        queue.offer(childA);
        queue.offer(childB);
      } else {
        List<Source> sources = ((MultipleOperator) op).getSources();
        for (Source source : sources) {
          Operator child = ((OperatorSource) source).getOperator();
          parents.put(child, op);
          queue.offer(child);
        }
      }
    }
    return new Pair<>(queue, parents);
  }

  private boolean isSelf(IginxMeta iginxMeta) {
    return iginxMeta.getIp().equals(selfIp) && iginxMeta.getPort() == selfPort;
  }
}
