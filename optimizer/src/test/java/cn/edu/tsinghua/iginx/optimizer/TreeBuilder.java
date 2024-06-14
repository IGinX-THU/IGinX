package cn.edu.tsinghua.iginx.optimizer;

import cn.edu.tsinghua.iginx.engine.shared.Constants;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OuterJoinType;
import cn.edu.tsinghua.iginx.engine.shared.source.EmptySource;
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.metadata.MetaManagerMock;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import java.util.*;

public class TreeBuilder {

  /*
     select
       |
     project
  */
  public static Operator buildSelectTree() {
    Project project = new Project(EmptySource.EMPTY_SOURCE, Collections.emptyList(), null);
    return new Select(new OperatorSource(project), new KeyFilter(Op.G, 10), null);
  }

  /*
           reorder
              |
           select
              |
          innerJoin
           /    \
      projectA projectB
  */
  public static Operator buildJoinTree0() {
    Project projectA = new Project(EmptySource.EMPTY_SOURCE, Collections.emptyList(), null);
    Project projectB = new Project(EmptySource.EMPTY_SOURCE, Collections.emptyList(), null);

    InnerJoin innerJoin =
        new InnerJoin(
            new OperatorSource(projectA),
            new OperatorSource(projectB),
            null,
            null,
            new PathFilter("test.a", Op.E, "test.b"),
            null);

    Select select = new Select(new OperatorSource(innerJoin), new KeyFilter(Op.G, 10), null);

    return new Reorder(new OperatorSource(select), Collections.singletonList("*"));
  }

  /*
             reorder
                |
             selectB
                |
            outerJoin
             /     \
        innerJoin projectC
          /    \
    projectA selectA
                |
             projectB
  */
  public static Operator buildJoinTree1() {
    Project projectA = new Project(EmptySource.EMPTY_SOURCE, Collections.emptyList(), null);
    Project projectB = new Project(EmptySource.EMPTY_SOURCE, Collections.emptyList(), null);

    Select selectA = new Select(new OperatorSource(projectB), new KeyFilter(Op.G, 100), null);

    InnerJoin innerJoin =
        new InnerJoin(
            new OperatorSource(projectA),
            new OperatorSource(selectA),
            null,
            null,
            new PathFilter("test.a", Op.E, "test.b"),
            null);

    Project projectC = new Project(EmptySource.EMPTY_SOURCE, Collections.emptyList(), null);
    OuterJoin outerJoin =
        new OuterJoin(
            new OperatorSource(innerJoin),
            new OperatorSource(projectC),
            null,
            null,
            OuterJoinType.FULL,
            new PathFilter("test.c", Op.E, "test.d"),
            null);

    Select selectB = new Select(new OperatorSource(outerJoin), new KeyFilter(Op.G, 10), null);

    return new Reorder(new OperatorSource(selectB), Collections.singletonList("*"));
  }

  public static Operator buildRemoveNotTree() {
    Project project = new Project(EmptySource.EMPTY_SOURCE, Collections.emptyList(), null);
    Select select =
        new Select(new OperatorSource(project), new NotFilter(new KeyFilter(Op.G, 100)), null);

    return new Reorder(new OperatorSource(select), Collections.singletonList("*"));
  }

  /**
   * 这棵树会被FFragmentPruningByFilterRule优化，优化前有10个Project-Fragment，优化后1个Project-Fragment。
   *
   * @return 形状如下的树
   */
  /*
                   Select
                     |
                   Join
                    / \
           Join      ....        Join
           / \                   / \
     Project Project .... Project Project
         |       |           |       |
    Fragment Fragment .... Fragment Fragment
  */
  public static Operator buildFragmentPruningByFilterRuleTree() {
    int keyInterval = 100;

    Queue<OperatorSource> operatorSourceQueue = new LinkedList<>();
    Map<ColumnsInterval, List<FragmentMeta>> fragmentsByColumnsInterval = new HashMap<>();

    // 生成Project-Fragment
    for (int startKey = 0; startKey < 1000; startKey += 100) {
      StorageUnitMeta storageUnitMeta =
          new StorageUnitMeta("fakeUnit" + startKey, 1, "fakeUnit" + startKey, false);

      String startPrefix = "test.a", endPrefix = "test.b";
      if (startKey % 200 == 0) {
        startPrefix = "test.b";
        endPrefix = null;
      }
      FragmentMeta fragmentMeta =
          new FragmentMeta(
              startPrefix, endPrefix, startKey, startKey + keyInterval, storageUnitMeta);
      FragmentSource fragmentSource = new FragmentSource(fragmentMeta);
      Project project = new Project(fragmentSource, Collections.singletonList("test.c"), null);
      operatorSourceQueue.add(new OperatorSource(project));

      ColumnsInterval columnsInterval = new ColumnsInterval(startPrefix, endPrefix);
      if (!fragmentsByColumnsInterval.containsKey(columnsInterval)) {
        fragmentsByColumnsInterval.put(columnsInterval, new ArrayList<>());
      }
      fragmentsByColumnsInterval.get(columnsInterval).add(fragmentMeta);
    }

    // 用Join(By Key)合成一个新的子树
    while (operatorSourceQueue.size() > 1) {
      OperatorSource operatorSource1 = operatorSourceQueue.poll();
      OperatorSource operatorSource2 = operatorSourceQueue.poll();
      Join join = new Join(operatorSource1, operatorSource2, Constants.KEY);
      operatorSourceQueue.add(new OperatorSource(join));
    }

    List<Filter> filters = Arrays.asList(new KeyFilter(Op.G, 180), new KeyFilter(Op.L, 220));

    Select select = new Select(operatorSourceQueue.poll(), new AndFilter(filters), null);

    // 构建MetaManagerMock
    MetaManagerMock metaManagerMock = MetaManagerMock.getInstance();
    metaManagerMock.setGetFragmentMapByColumnsIntervalMockMap(fragmentsByColumnsInterval);

    return new Reorder(new OperatorSource(select), Collections.singletonList("*"));
  }

  /**
   * 这棵树不会被FragmentPruningByFilterRule优化，因为Select节点的子树下包含会被FragmentPruningByFilterRule跳过的节点。
   *
   * @return 形状如下的树
   */
  /*
                    Select
                       |
                  InnerJoin
                     / \
            InnerJoin   ....   InnerJoin
             / \                   / \
     Project Project .... Project Project
           |       |           |       |
    Fragment Fragment .... Fragment Fragment
  */
  public static Operator buildFragmentPruningByFilterRuleTreeContainsInvalidOperator() {
    int keyInterval = 100;

    Queue<OperatorSource> operatorSourceQueue = new LinkedList<>();
    Map<ColumnsInterval, List<FragmentMeta>> fragmentsByColumnsInterval = new HashMap<>();

    // 生成Project-Fragment
    for (int startKey = 0; startKey < 400; startKey += 100) {
      StorageUnitMeta storageUnitMeta =
          new StorageUnitMeta("fakeUnit" + startKey, 1, "fakeUnit" + startKey, false);

      String startPrefix = "test.a", endPrefix = "test.b";
      if (startKey % 200 == 0) {
        startPrefix = "test.b";
        endPrefix = null;
      }
      FragmentMeta fragmentMeta =
          new FragmentMeta(
              startPrefix, endPrefix, startKey, startKey + keyInterval, storageUnitMeta);
      FragmentSource fragmentSource = new FragmentSource(fragmentMeta);
      Project project = new Project(fragmentSource, Collections.singletonList("test.c"), null);
      operatorSourceQueue.add(new OperatorSource(project));

      ColumnsInterval columnsInterval = new ColumnsInterval(startPrefix, endPrefix);
      if (!fragmentsByColumnsInterval.containsKey(columnsInterval)) {
        fragmentsByColumnsInterval.put(columnsInterval, new ArrayList<>());
      }
      fragmentsByColumnsInterval.get(columnsInterval).add(fragmentMeta);
    }

    // 用InnerJoin合成一个新的子树
    while (operatorSourceQueue.size() > 1) {
      OperatorSource operatorSource1 = operatorSourceQueue.poll();
      OperatorSource operatorSource2 = operatorSourceQueue.poll();
      CrossJoin crossJoin = new CrossJoin(operatorSource1, operatorSource2, "test.a", "test.a");
      operatorSourceQueue.add(new OperatorSource(crossJoin));
    }

    List<Filter> filters = Arrays.asList(new KeyFilter(Op.G, 180), new KeyFilter(Op.L, 220));

    Select select = new Select(operatorSourceQueue.poll(), new AndFilter(filters), null);

    // 构建MetaManagerMock
    MetaManagerMock metaManagerMock = MetaManagerMock.getInstance();
    metaManagerMock.setGetFragmentMapByColumnsIntervalMockMap(fragmentsByColumnsInterval);

    return new Reorder(new OperatorSource(select), Collections.singletonList("*"));
  }
}
