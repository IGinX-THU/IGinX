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
package cn.edu.tsinghua.iginx.filesystem.struct.tree.query;

import cn.edu.tsinghua.iginx.filesystem.struct.DataTarget;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import lombok.Getter;

@Getter
public abstract class AbstractQuerier implements Querier {

  private final Path path;
  private final String prefix;
  private final DataTarget target;
  private final ExecutorService executor;

  protected AbstractQuerier() {
    this(null, null, null, null);
  }

  protected AbstractQuerier(Path path, String prefix, DataTarget target, ExecutorService executor) {
    this.path = path;
    this.prefix = prefix;
    this.target = target;
    this.executor = executor;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "://" + path + "?prefix=" + prefix + "&target=" + target;
  }
}
