package cn.edu.tsinghua.iginx.optimizer;

import static cn.edu.tsinghua.iginx.logical.optimizer.rules.Rule.any;
import static cn.edu.tsinghua.iginx.logical.optimizer.rules.Rule.operand;

import cn.edu.tsinghua.iginx.engine.shared.operator.AbstractJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.engine.shared.operator.Reorder;
import cn.edu.tsinghua.iginx.engine.shared.operator.Select;
import cn.edu.tsinghua.iginx.engine.shared.source.EmptySource;
import cn.edu.tsinghua.iginx.logical.optimizer.core.Operand;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Test;

public class OperandTest {

  @Test
  public void testMatchAny() {
    // ANY_OPERAND matches any sub tree.
    Operand expected = any();
    Operator actual0 = TreeBuilder.buildJoinTree0();
    Operator actual1 = new Reorder(new EmptySource(), Collections.singletonList("*"));
    Operator actual2 = new Project(EmptySource.EMPTY_SOURCE, Collections.emptyList(), null);

    Assert.assertTrue(expected.matches(actual0));
    Assert.assertTrue(expected.matches(actual1));
    Assert.assertTrue(expected.matches(actual2));
  }

  @Test
  public void testMatchSingleOperator() {
    Operand expected = operand(Project.class, any());
    Operator actual = new Project(EmptySource.EMPTY_SOURCE, Collections.emptyList(), null);
    Assert.assertTrue(expected.matches(actual));

    expected = operand(Select.class, any());
    actual = TreeBuilder.buildSelectTree();
    Assert.assertTrue(expected.matches(actual));
  }

  @Test
  public void testMatchSubTree() {
    Operand expected =
        operand(Reorder.class, operand(Select.class, operand(AbstractJoin.class, any(), any())));
    Operator actual = TreeBuilder.buildJoinTree0();
    Assert.assertTrue(expected.matches(actual));
  }

  @Test
  public void testMismatch() {
    Operand expected = operand(Select.class, operand(AbstractJoin.class, any(), any()));
    Operator actual = TreeBuilder.buildJoinTree0();
    Assert.assertFalse(expected.matches(actual));
  }
}
