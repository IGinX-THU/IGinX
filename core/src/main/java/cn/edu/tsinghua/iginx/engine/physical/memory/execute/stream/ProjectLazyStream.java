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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream;

import static cn.edu.tsinghua.iginx.engine.shared.Constants.KEY;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class ProjectLazyStream extends UnaryLazyStream {

  private final Project project;

  private Header header;

  private Row nextRow = null;

  public ProjectLazyStream(Project project, RowStream stream) {
    super(stream);
    this.project = project;
  }

  @Override
  public Header getHeader() throws PhysicalException {
    if (header == null) {
      List<String> patterns = project.getPatterns();
      Header header = stream.getHeader();
      List<Field> targetFields = new ArrayList<>();

      for (Field field : header.getFields()) {
        if (project.isRemainKey() && field.getName().endsWith(KEY)) {
          targetFields.add(field);
          continue;
        }
        for (String pattern : patterns) {
          if (!StringUtils.isPattern(pattern)) {
            if (pattern.equals(field.getFullName())) {
              targetFields.add(field);
            }
          } else {
            if (Pattern.matches(StringUtils.reformatPath(pattern), field.getFullName())) {
              targetFields.add(field);
            }
          }
        }
      }
      this.header = new Header(header.getKey(), targetFields);
    }
    return header;
  }

  @Override
  public boolean hasNext() throws PhysicalException {
    if (nextRow == null) {
      nextRow = calculateNext();
    }
    return nextRow != null;
  }

  private Row calculateNext() throws PhysicalException {
    Header header = getHeader();
    List<Field> fields = header.getFields();
    while (stream.hasNext()) {
      Row row = stream.next();
      Object[] objects = new Object[fields.size()];
      boolean allNull = true;
      for (int i = 0; i < fields.size(); i++) {
        objects[i] = row.getValue(fields.get(i));
        if (allNull && objects[i] != null) {
          allNull = false;
        }
      }
      if (allNull) {
        continue;
      }
      if (header.hasKey()) {
        return new Row(header, row.getKey(), objects);
      } else {
        return new Row(header, objects);
      }
    }
    return null;
  }

  @Override
  public Row next() throws PhysicalException {
    if (!hasNext()) {
      throw new IllegalStateException("row stream doesn't have more data!");
    }
    Row row = nextRow;
    nextRow = null;
    return row;
  }
}
