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

import cn.edu.tsinghua.iginx.engine.physical.storage.utils.TagKVUtils;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.checkerframework.checker.nullness.qual.Nullable;

public class CompactInvertedTagsSet {
  private InvertedTagsSet tagsSet;

  public CompactInvertedTagsSet(Map<String, String> tags) {
    if (tags.isEmpty()) {
      this.tagsSet = null;
    } else {
      this.tagsSet = new InvertedTagsSet();
      this.tagsSet.add(tags);
    }
  }

  public CompactInvertedTagsSet(Collection<Map<String, String>> tagsCollection) {
      this.tagsSet = new InvertedTagsSet();
      for (Map<String, String> tags : tagsCollection) {
        this.tagsSet.add(tags);
      }
  }

  public void add(Map<String, String> tags) {
    if (tagsSet == null) {
      if (tags.isEmpty()) {
        return;
      }
      tagsSet = new InvertedTagsSet();
      tagsSet.add(Collections.emptyMap());
    }
    tagsSet.add(tags);
  }

  public boolean contain(Map<String, String> tags) {
    if (tagsSet == null) {
      return tags.isEmpty();
    }
    return tagsSet.contain(tags);
  }

  public boolean isEmpty() {
    return tagsSet != null && tagsSet.isEmpty();
  }

  public void remove(Map<String, String> tags) {
    if (tagsSet == null) {
      if (tags.isEmpty()) {
        tagsSet = new InvertedTagsSet();
      }
      return;
    }
    tagsSet.remove(tags);
  }

  public Set<Map<String, String>> find(@Nullable TagFilter filter) {
    if (tagsSet == null) {
      if (filter == null || TagKVUtils.match(Collections.emptyMap(), filter)) {
        return Collections.singleton(Collections.emptyMap());
      } else {
        return Collections.emptySet();
      }
    } else {
      return tagsSet.find(filter);
    }
  }

  public int size() {
    if (tagsSet == null) {
      return 1;
    } else {
      return tagsSet.size();
    }
  }
}
