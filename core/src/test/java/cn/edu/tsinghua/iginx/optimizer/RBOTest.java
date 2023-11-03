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
import org.junit.Test;

public class RBOTest {

    private final RuleBasedOptimizer rbo = new RuleBasedOptimizer();

    @Test
    public void testFilterJoinTransposeRule() {
        Project projectA = new Project(EmptySource.EMPTY_SOURCE, Collections.emptyList(), null);
        Project projectB = new Project(EmptySource.EMPTY_SOURCE, Collections.emptyList(), null);

        InnerJoin innerJoin = new InnerJoin(new OperatorSource(projectA), new OperatorSource(projectB), null, null, new PathFilter("test.a", Op.E, "test.b"), null);

        Select select = new Select(new OperatorSource(innerJoin), new KeyFilter(Op.G, 10), null);

        Reorder reorder = new Reorder(new OperatorSource(select), Collections.singletonList("*"));
        System.out.println(reorder.getInfo());

        Operator root = rbo.optimize(reorder);
        System.out.println(root.getInfo());
    }
}
