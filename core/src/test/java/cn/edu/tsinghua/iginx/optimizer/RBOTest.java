package cn.edu.tsinghua.iginx.optimizer;

import cn.edu.tsinghua.iginx.engine.logical.optimizer.rbo.RuleBasedOptimizer;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import org.junit.Assert;
import org.junit.Test;

public class RBOTest {

  private final RuleBasedOptimizer rbo = new RuleBasedOptimizer();

  @Test
  public void testFilterJoinTransposeRule() {
    Operator root = TreeBuilder.buildJoinTree0();
    String expected =
        "[Reorder] Order: *\n"
            + "  [Select] Filter: key > 10\n"
            + "    [InnerJoin] PrefixA: null, PrefixB: null, IsNatural: false, Filter: test.a == test.b\n"
            + "      [Project] Patterns:\n"
            + "      [Project] Patterns:\n";
    String actual = TreePrinter.getTreeInfo(root);
    Assert.assertEquals(expected, actual);

    Operator rootAfterRBO = rbo.optimize(root);
    expected =
        "[Reorder] Order: *\n"
            + "  [InnerJoin] PrefixA: null, PrefixB: null, IsNatural: false, Filter: test.a == test.b\n"
            + "    [Select] Filter: key > 10\n"
            + "      [Project] Patterns:\n"
            + "    [Select] Filter: key > 10\n"
            + "      [Project] Patterns:\n";
    actual = TreePrinter.getTreeInfo(rootAfterRBO);
    Assert.assertEquals(expected, actual);
  }
}
