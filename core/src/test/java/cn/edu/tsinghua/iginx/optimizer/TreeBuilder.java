package cn.edu.tsinghua.iginx.optimizer;

import cn.edu.tsinghua.iginx.engine.shared.operator.InnerJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.OuterJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.engine.shared.operator.Reorder;
import cn.edu.tsinghua.iginx.engine.shared.operator.Select;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.KeyFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.NotFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.PathFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OuterJoinType;
import cn.edu.tsinghua.iginx.engine.shared.source.EmptySource;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import java.util.Collections;

public class TreeBuilder {

  /** select | project */
  public static Operator buildSelectTree() {
    Project project = new Project(EmptySource.EMPTY_SOURCE, Collections.emptyList(), null);
    return new Select(new OperatorSource(project), new KeyFilter(Op.G, 10), null);
  }

  /** reorder | select | innerJoin / \ projectA projectB */
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

  /** reorder | selectB | outerJoin / \ innerJoin projectC / \ projectA selectA | projectB */
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
}
