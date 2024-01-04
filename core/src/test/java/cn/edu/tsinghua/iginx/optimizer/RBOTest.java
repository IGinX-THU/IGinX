package cn.edu.tsinghua.iginx.optimizer;

import cn.edu.tsinghua.iginx.engine.logical.optimizer.rbo.RuleBasedOptimizer;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import org.junit.Assert;
import org.junit.Test;

public class RBOTest {

  private final RuleBasedOptimizer rbo = new RuleBasedOptimizer();

  @Test
  public void testRemoveNotRule() {
    Operator root = TreeBuilder.buildRemoveNotTree();
    String expected =
        "[Reorder] Order: *\n" + "  [Select] Filter: !(key > 100)\n" + "    [Project] Patterns:\n";
    String actual = TreePrinter.getTreeInfo(root);
    Assert.assertEquals(expected, actual);

    Operator rootAfterRBO = rbo.optimize(root);
    expected =
        "[Reorder] Order: *\n" + "  [Select] Filter: key <= 100\n" + "    [Project] Patterns:\n";
    actual = TreePrinter.getTreeInfo(rootAfterRBO);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testFilterFragmentRule() {
    // 下面这棵树会被优化
    Operator root = TreeBuilder.buildFilterFragmentTree();

    String expected =
        "[Reorder] Order: *\n"
            + "  [Select] Filter: (key > 180 && key < 220)\n"
            + "    [Join] JoinBy: key\n"
            + "      [Join] JoinBy: key\n"
            + "        [Join] JoinBy: key\n"
            + "          [Project] Patterns: test.c, Target DU: fakeUnit400\n"
            + "          [Project] Patterns: test.c, Target DU: fakeUnit500\n"
            + "        [Join] JoinBy: key\n"
            + "          [Project] Patterns: test.c, Target DU: fakeUnit600\n"
            + "          [Project] Patterns: test.c, Target DU: fakeUnit700\n"
            + "      [Join] JoinBy: key\n"
            + "        [Join] JoinBy: key\n"
            + "          [Project] Patterns: test.c, Target DU: fakeUnit800\n"
            + "          [Project] Patterns: test.c, Target DU: fakeUnit900\n"
            + "        [Join] JoinBy: key\n"
            + "          [Join] JoinBy: key\n"
            + "            [Project] Patterns: test.c, Target DU: fakeUnit0\n"
            + "            [Project] Patterns: test.c, Target DU: fakeUnit100\n"
            + "          [Join] JoinBy: key\n"
            + "            [Project] Patterns: test.c, Target DU: fakeUnit200\n"
            + "            [Project] Patterns: test.c, Target DU: fakeUnit300\n";
    String actual = TreePrinter.getTreeInfo(root);
    Assert.assertEquals(expected, actual);

    Operator rootAfterRBO = rbo.optimize(root);
    expected =
        "[Reorder] Order: *\n"
            + "  [Select] Filter: (key > 180 && key < 220)\n"
            + "    [Project] Patterns: test.c, Target DU: fakeUnit200\n";
    actual = TreePrinter.getTreeInfo(rootAfterRBO);
    Assert.assertEquals(expected, actual);

    // 下面这棵树由于包含无法通过FilterFragmentRule的Matches,包含了不能被优化的节点，不会被优化
    root = TreeBuilder.buildFilterFragmentTreeContainsInvalidOperator();
    expected =
        "[Reorder] Order: *\n"
            + "  [Select] Filter: (key > 180 && key < 220)\n"
            + "    [CrossJoin] PrefixA: test.a, PrefixB: test.a\n"
            + "      [CrossJoin] PrefixA: test.a, PrefixB: test.a\n"
            + "        [Project] Patterns: test.c, Target DU: fakeUnit0\n"
            + "        [Project] Patterns: test.c, Target DU: fakeUnit100\n"
            + "      [CrossJoin] PrefixA: test.a, PrefixB: test.a\n"
            + "        [Project] Patterns: test.c, Target DU: fakeUnit200\n"
            + "        [Project] Patterns: test.c, Target DU: fakeUnit300\n";
    actual = TreePrinter.getTreeInfo(root);
    Assert.assertEquals(expected, actual);

    rootAfterRBO = rbo.optimize(root);
    actual = TreePrinter.getTreeInfo(rootAfterRBO);
    Assert.assertEquals(expected, actual);
  }
}
