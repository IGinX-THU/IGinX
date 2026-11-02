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
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util;

import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractTable implements Table {

  @Override
  public abstract String toString();

  @Override
  public Meta getMeta() throws IOException {
    List<Meta> subMetas = new ArrayList<>();
    for (SubTable subTable : getSubTables()) {
      subMetas.add(subTable.getMeta());
    }
    return mergeMeta(subMetas);
  }

  private static Meta mergeMeta(Iterable<Meta> subMetas) {
    HashMap<Field, Statistic> fieldStats = new HashMap<>();
    for (Meta subMeta : subMetas) {
      Map<Field, Statistic> subFieldStats = subMeta.getFieldStats();
      for (Map.Entry<Field, Statistic> entry : subFieldStats.entrySet()) {
        Field field = entry.getKey();
        Statistic statistic = entry.getValue();
        fieldStats.merge(field, statistic, AbstractTable::mergeStatistic);
      }
    }
    return new Meta(ImmutableMap.copyOf(fieldStats));
  }

  private static Statistic mergeStatistic(Statistic s1, Statistic s2) {
    return new Statistic(s1.getKeyRange().span(s2.getKeyRange()));
  }
}
