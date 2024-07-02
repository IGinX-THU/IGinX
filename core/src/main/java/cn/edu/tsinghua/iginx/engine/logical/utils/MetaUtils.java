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
package cn.edu.tsinghua.iginx.engine.logical.utils;

import static cn.edu.tsinghua.iginx.metadata.utils.FragmentUtils.keyFromColumnsIntervalToKeyInterval;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.shared.operator.AddSchemaPrefix;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import cn.edu.tsinghua.iginx.policy.IPolicy;
import cn.edu.tsinghua.iginx.policy.PolicyManager;
import cn.edu.tsinghua.iginx.sql.statement.select.SelectStatement;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MetaUtils {

  private static final IMetaManager metaManager = DefaultMetaManager.getInstance();

  private static final IPolicy policy =
      PolicyManager.getInstance()
          .getPolicy(ConfigDescriptor.getInstance().getConfig().getPolicyClassName());

  public static Operator mergeRawData(
      Map<KeyInterval, List<FragmentMeta>> fragments,
      List<FragmentMeta> dummyFragments,
      List<String> pathList,
      TagFilter tagFilter) {
    List<Operator> unionList = new ArrayList<>();
    fragments.forEach(
        (k, v) -> {
          List<Operator> joinList = new ArrayList<>();
          v.forEach(
              meta -> joinList.add(new Project(new FragmentSource(meta), pathList, tagFilter)));
          unionList.add(OperatorUtils.joinOperatorsByTime(joinList));
        });

    Operator operator = OperatorUtils.unionOperators(unionList);
    if (!dummyFragments.isEmpty()) {
      List<Operator> joinList = new ArrayList<>();
      dummyFragments.forEach(
          meta -> {
            if (meta.isValid()) {
              String schemaPrefix = meta.getColumnsInterval().getSchemaPrefix();
              joinList.add(
                  new AddSchemaPrefix(
                      new OperatorSource(
                          new Project(
                              new FragmentSource(meta),
                              pathMatchPrefix(pathList, meta.getColumnsInterval(), schemaPrefix),
                              tagFilter)),
                      schemaPrefix));
            }
          });
      if (operator != null) {
        joinList.add(operator);
      }
      operator = OperatorUtils.joinOperatorsByTime(joinList);
    }
    return operator;
  }

  public static Pair<Map<KeyInterval, List<FragmentMeta>>, List<FragmentMeta>>
      getFragmentsByColumnsInterval(
          SelectStatement selectStatement, ColumnsInterval columnsInterval) {
    Map<ColumnsInterval, List<FragmentMeta>> fragmentsByColumnsInterval =
        metaManager.getFragmentMapByColumnsInterval(
            PathUtils.trimColumnsInterval(columnsInterval), true);
    if (!metaManager.hasFragment()) {
      if (metaManager.hasWritableStorageEngines()) {
        // on startup
        Pair<List<FragmentMeta>, List<StorageUnitMeta>> fragmentsAndStorageUnits =
            policy.generateInitialFragmentsAndStorageUnits(selectStatement);
        metaManager.createInitialFragmentsAndStorageUnits(
            fragmentsAndStorageUnits.v, fragmentsAndStorageUnits.k);
      }
      fragmentsByColumnsInterval =
          metaManager.getFragmentMapByColumnsInterval(columnsInterval, true);
    }
    return keyFromColumnsIntervalToKeyInterval(fragmentsByColumnsInterval);
  }

  // 筛选出在 columnsInterval 范围内的 path 列表，返回去除 schemaPrefix 后的结果
  private static List<String> pathMatchPrefix(
      List<String> pathList, ColumnsInterval columnsInterval, String schemaPrefix) {
    List<String> ans = new ArrayList<>();
    boolean hasMoreDelimiter = false;

    for (String path : pathList) {
      String pathWithoutPrefix = path;
      if (path.equals("*.*") || path.equals("*")) {
        ans.add(path);
        continue;
      }
      if (schemaPrefix != null) {
        if (!path.startsWith(schemaPrefix) && !path.startsWith("*")) {
          continue;
        }
        if (path.startsWith(schemaPrefix)) {
          pathWithoutPrefix = path.substring(schemaPrefix.length() + 1);
        } else if (path.startsWith("*.*")) {
          pathWithoutPrefix = path.substring(2);
        } else if (path.startsWith("*") && path.indexOf(".", path.indexOf(".") + 1) != -1) {
          hasMoreDelimiter = true;
          pathWithoutPrefix = path.substring(2);
        }
      }
      if (columnsInterval.isContain(path)) {
        if (path.startsWith("*") && hasMoreDelimiter) {
          ans.add(path);
        }
        ans.add(pathWithoutPrefix);
      }
    }

    return ans;
  }
}
