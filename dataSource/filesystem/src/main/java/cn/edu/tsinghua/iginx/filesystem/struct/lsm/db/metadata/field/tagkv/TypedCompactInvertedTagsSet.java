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
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.metadata.field.tagkv;

import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.exception.TypeConflictedException;

import java.util.Collection;
import java.util.Map;

import cn.edu.tsinghua.iginx.thrift.DataType;

public class TypedCompactInvertedTagsSet extends CompactInvertedTagsSet {
  private final DataType type;

  public TypedCompactInvertedTagsSet(DataType type, Map<String, String> tags) {
    super(tags);
    this.type = type;
  }

  public TypedCompactInvertedTagsSet(DataType type, Collection<Map<String, String>> tagsCollection) {
    super(tagsCollection);
    this.type = type;
  }


  public DataType getType() {
    return type;
  }

  public void add(Map<String, String> tags, DataType type) throws TypeConflictedException {
    if (this.type != type) {
      throw new TypeConflictedException(tags.toString(), type.toString(), this.type.toString());
    }
    super.add(tags);
  }

  public boolean contain(Map<String, String> tags, DataType type)
      throws TypeConflictedException {
    if (this.type != type) {
      throw new TypeConflictedException(tags.toString(), type.toString(), this.type.toString());
    }
    return super.contain(tags);
  }

  public void remove(Map<String, String> tags, DataType type)
      throws TypeConflictedException {
    if (this.type != type) {
      throw new TypeConflictedException(tags.toString(), type.toString(), this.type.toString());
    }
    super.remove(tags);
  }
}
