/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.filesystem.common;

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
