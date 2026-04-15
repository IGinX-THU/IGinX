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
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.Table;
import com.google.common.collect.RangeSet;
import java.io.IOException;
import java.util.List;
import java.util.Set;

public interface StorageManager {

  void flush(long tableId, Table table) throws IOException, PhysicalException;

  Table read(long tableId) throws IOException;

  void delete(long tableId) throws IOException;

  void delete(long tableId, Set<Field> fields) throws IOException;

  void delete(long tableId, Set<Field> fields, RangeSet<Long> keyRangeSet) throws IOException;

  Iterable<Long> list() throws IOException;

  void clear() throws IOException;
}
