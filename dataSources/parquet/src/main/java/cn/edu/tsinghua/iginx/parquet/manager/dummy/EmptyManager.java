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
package cn.edu.tsinghua.iginx.parquet.manager.dummy;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Column;
import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.parquet.manager.Manager;
import java.util.Collections;
import java.util.List;

public class EmptyManager implements Manager {
  @Override
  public RowStream project(List<String> paths, TagFilter tagFilter, Filter filter)
      throws PhysicalException {
    return new RowStream() {

      private final Header header = new Header(Field.KEY, Collections.emptyList());

      @Override
      public Header getHeader() {
        return header;
      }

      @Override
      public void close() {}

      @Override
      public boolean hasNext() {
        return false;
      }

      @Override
      public Row next() {
        return null;
      }
    };
  }

  @Override
  public void insert(DataView dataView) throws PhysicalException {
    throw new PhysicalException("insert is not supported");
  }

  @Override
  public void delete(List<String> paths, List<KeyRange> keyRanges, TagFilter tagFilter)
      throws PhysicalException {
    throw new PhysicalException("delete is not supported");
  }

  @Override
  public List<Column> getColumns() throws PhysicalException {
    return Collections.emptyList();
  }

  @Override
  public KeyInterval getKeyInterval() throws PhysicalException {
    return new KeyInterval(0, 0);
  }

  @Override
  public void close() throws Exception {}
}
