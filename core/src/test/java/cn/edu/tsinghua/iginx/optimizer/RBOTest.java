package cn.edu.tsinghua.iginx.optimizer;

import cn.edu.tsinghua.iginx.engine.logical.optimizer.rbo.RuleBasedOptimizer;
import cn.edu.tsinghua.iginx.engine.shared.operator.InnerJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.engine.shared.operator.Reorder;
import cn.edu.tsinghua.iginx.engine.shared.operator.Select;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.KeyFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.PathFilter;
import cn.edu.tsinghua.iginx.engine.shared.source.EmptySource;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Test;

public class RBOTest {

  private final RuleBasedOptimizer rbo = new RuleBasedOptimizer();

  @Test
  public void testFilterJoinTransposeRule() {
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

    Reorder reorder = new Reorder(new OperatorSource(select), Collections.singletonList("*"));
    String expected =
        "[Reorder] Order: *\n"
            + "  [Select] Filter: key > 10\n"
            + "    [InnerJoin] PrefixA: null, PrefixB: null, IsNatural: false, Filter: test.a == test.b\n"
            + "      [Project] Patterns:\n"
            + "      [Project] Patterns:\n";
    String actual = TreePrinter.getTreeInfo(reorder);
    Assert.assertEquals(expected, actual);

    Operator root = rbo.optimize(reorder);
    expected =
        "[Reorder] Order: *\n"
            + "  [InnerJoin] PrefixA: null, PrefixB: null, IsNatural: false, Filter: test.a == test.b\n"
            + "    [Select] Filter: key > 10\n"
            + "      [Project] Patterns:\n"
            + "    [Select] Filter: key > 10\n"
            + "      [Project] Patterns:\n";
    actual = TreePrinter.getTreeInfo(root);
    Assert.assertEquals(expected, actual);
  }
}
