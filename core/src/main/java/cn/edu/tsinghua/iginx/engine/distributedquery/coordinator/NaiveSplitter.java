package cn.edu.tsinghua.iginx.engine.distributedquery.coordinator;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
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

    return new Plan(root, subPlans);
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
