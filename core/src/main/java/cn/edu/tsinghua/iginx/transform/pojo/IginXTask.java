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
package cn.edu.tsinghua.iginx.transform.pojo;

import cn.edu.tsinghua.iginx.thrift.TaskInfo;
import cn.edu.tsinghua.iginx.utils.TaskFromYAML;
import java.util.ArrayList;
import java.util.List;

public class IginXTask extends Task {

  private final List<String> sqlList = new ArrayList<>();

  public IginXTask(TaskInfo info) {
    super(info);
    if (info.isSetSqlList()) {
      sqlList.addAll(info.getSqlList());
    } else {
      throw new IllegalArgumentException("IginX task must have a SQL statement.");
    }
  }

  public IginXTask(TaskFromYAML info) {
    super(info);
    if (info.getSqlList() != null && !info.getSqlList().isEmpty()) {
      sqlList.addAll(info.getSqlList());
    } else {
      throw new IllegalArgumentException("IginX task must have a SQL statement.");
    }
  }

  public List<String> getSqlList() {
    return sqlList;
  }
}
