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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.filesystem.format;

import com.google.common.collect.Lists;
import java.util.*;

public abstract class AbstractFileFormat implements FileFormat {

  protected final String formatName;
  protected final Collection<String> extensions;

  public AbstractFileFormat(String formatName, String... extension) {
    this.formatName = Objects.requireNonNull(formatName);
    Collection<String> extensions = new HashSet<>();
    for (String ext : extension) {
      extensions.add(Objects.requireNonNull(ext));
    }
    this.extensions = Collections.unmodifiableCollection(extensions);
  }

  @Override
  public List<String> getExtensions() {
    return Lists.newArrayList(extensions);
  }
}
