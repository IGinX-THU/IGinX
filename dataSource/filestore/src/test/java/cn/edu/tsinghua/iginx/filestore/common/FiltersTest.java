package cn.edu.tsinghua.iginx.filestore.common;

import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import java.util.Arrays;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class FiltersTest {

  @Test
  void testSuperSet() {
    AndFilter root =
        new AndFilter(
            Arrays.asList(
                new OrFilter(Arrays.asList(new KeyFilter(Op.GE, 1), new KeyFilter(Op.LE, 3))),
                new OrFilter(
                    Arrays.asList(
                        new ValueFilter("name", Op.E, new Value("Alice")),
                        new ValueFilter("age", Op.G, new Value(18))))));
    Filter superSet = Filters.superSet(root, Filters.nonKeyFilter());
    Filter expected = new OrFilter(Arrays.asList(new KeyFilter(Op.GE, 1), new KeyFilter(Op.LE, 3)));

    Assertions.assertEquals(expected, superSet);
  }
}
