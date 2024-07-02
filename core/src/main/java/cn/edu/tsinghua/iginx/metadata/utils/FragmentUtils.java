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
package cn.edu.tsinghua.iginx.metadata.utils;

import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.*;

public class FragmentUtils {

  public static Pair<Map<KeyInterval, List<FragmentMeta>>, List<FragmentMeta>>
      keyFromColumnsIntervalToKeyInterval(
          Map<ColumnsInterval, List<FragmentMeta>> fragmentMapByColumnsInterval) {
    Map<KeyInterval, List<FragmentMeta>> fragmentMapByKeyInterval = new HashMap<>();
    List<FragmentMeta> dummyFragments = new ArrayList<>();
    fragmentMapByColumnsInterval.forEach(
        (k, v) ->
            v.forEach(
                fragmentMeta -> {
                  if (fragmentMeta.isDummyFragment()) {
                    dummyFragments.add(fragmentMeta);
                    return;
                  }
                  if (fragmentMapByKeyInterval.containsKey(fragmentMeta.getKeyInterval())) {
                    fragmentMapByKeyInterval.get(fragmentMeta.getKeyInterval()).add(fragmentMeta);
                  } else {
                    fragmentMapByKeyInterval.put(
                        fragmentMeta.getKeyInterval(),
                        new ArrayList<>(Collections.singletonList(fragmentMeta)));
                  }
                }));
    return new Pair<>(fragmentMapByKeyInterval, dummyFragments);
  }
}
