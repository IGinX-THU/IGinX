/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.optimizer;

import cn.edu.tsinghua.iginx.engine.shared.operator.InnerJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.OuterJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.engine.shared.operator.Reorder;
import cn.edu.tsinghua.iginx.engine.shared.operator.Select;
import cn.edu.tsinghua.iginx.logical.optimizer.core.iterator.*;
import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class IteratorTest {

  @Test
  public void testDeepFirstIterator() {
    Operator root = TreeBuilder.buildSelectTree();
    TreeIterator it = new DeepFirstIterator(root);
    List<Class<? extends Operator>> expectedOrder = Arrays.asList(Select.class, Project.class);
    Assert.assertTrue(matchExpectedOrder(expectedOrder, it));

    root = TreeBuilder.buildJoinTree0();
    it = new DeepFirstIterator(root);
    expectedOrder =
        Arrays.asList(Reorder.class, Select.class, InnerJoin.class, Project.class, Project.class);
    Assert.assertTrue(matchExpectedOrder(expectedOrder, it));

    root = TreeBuilder.buildJoinTree1();
    it = new DeepFirstIterator(root);
    expectedOrder =
        Arrays.asList(
            Reorder.class,
            Select.class,
            OuterJoin.class,
            InnerJoin.class,
            Project.class,
            Select.class,
            Project.class,
            Project.class);
    Assert.assertTrue(matchExpectedOrder(expectedOrder, it));
  }

  @Test
  public void testReverseDeepFirstIterator() {
    Operator root = TreeBuilder.buildSelectTree();
    TreeIterator it = new ReverseDeepFirstIterator(root);
    List<Class<? extends Operator>> expectedOrder = Arrays.asList(Project.class, Select.class);
    Assert.assertTrue(matchExpectedOrder(expectedOrder, it));

    root = TreeBuilder.buildJoinTree0();
    it = new ReverseDeepFirstIterator(root);
    expectedOrder =
        Arrays.asList(Project.class, Project.class, InnerJoin.class, Select.class, Reorder.class);
    Assert.assertTrue(matchExpectedOrder(expectedOrder, it));

    root = TreeBuilder.buildJoinTree1();
    it = new ReverseDeepFirstIterator(root);
    expectedOrder =
        Arrays.asList(
            Project.class,
            Project.class,
            Select.class,
            Project.class,
            InnerJoin.class,
            OuterJoin.class,
            Select.class,
            Reorder.class);
    Assert.assertTrue(matchExpectedOrder(expectedOrder, it));
  }

  @Test
  public void testLeveledIterator() {
    Operator root = TreeBuilder.buildSelectTree();
    TreeIterator it = new LeveledIterator(root);
    List<Class<? extends Operator>> expectedOrder = Arrays.asList(Select.class, Project.class);
    Assert.assertTrue(matchExpectedOrder(expectedOrder, it));

    root = TreeBuilder.buildJoinTree0();
    it = new LeveledIterator(root);
    expectedOrder =
        Arrays.asList(Reorder.class, Select.class, InnerJoin.class, Project.class, Project.class);
    Assert.assertTrue(matchExpectedOrder(expectedOrder, it));

    root = TreeBuilder.buildJoinTree1();
    it = new LeveledIterator(root);
    expectedOrder =
        Arrays.asList(
            Reorder.class,
            Select.class,
            OuterJoin.class,
            InnerJoin.class,
            Project.class,
            Project.class,
            Select.class,
            Project.class);
    Assert.assertTrue(matchExpectedOrder(expectedOrder, it));
  }

  @Test
  public void testReverseLeveledIterator() {
    Operator root = TreeBuilder.buildSelectTree();
    TreeIterator it = new ReverseLeveledIterator(root);
    List<Class<? extends Operator>> expectedOrder = Arrays.asList(Project.class, Select.class);
    Assert.assertTrue(matchExpectedOrder(expectedOrder, it));

    root = TreeBuilder.buildJoinTree0();
    it = new ReverseLeveledIterator(root);
    expectedOrder =
        Arrays.asList(Project.class, Project.class, InnerJoin.class, Select.class, Reorder.class);
    Assert.assertTrue(matchExpectedOrder(expectedOrder, it));

    root = TreeBuilder.buildJoinTree1();
    it = new ReverseLeveledIterator(root);
    expectedOrder =
        Arrays.asList(
            Project.class,
            Select.class,
            Project.class,
            Project.class,
            InnerJoin.class,
            OuterJoin.class,
            Select.class,
            Reorder.class);
    Assert.assertTrue(matchExpectedOrder(expectedOrder, it));
  }

  private boolean matchExpectedOrder(List<Class<? extends Operator>> opList, TreeIterator it) {
    int index = 0;
    while (index < opList.size() && it.hasNext()) {
      Operator op = it.next();
      if (!opList.get(index).isInstance(op)) {
        return false;
      }
      index++;
    }
    return index >= opList.size() && !it.hasNext();
  }
}
