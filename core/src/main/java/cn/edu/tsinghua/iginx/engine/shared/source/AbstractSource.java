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
package cn.edu.tsinghua.iginx.engine.shared.source;

public abstract class AbstractSource implements Source {

  private final SourceType type;

  public AbstractSource(SourceType type) {
    if (type == null) {
      throw new IllegalArgumentException("source type shouldn't be null");
    }
    this.type = type;
  }

  public AbstractSource() {
    this.type = SourceType.Unknown;
  }

  @Override
  public SourceType getType() {
    return type;
  }
}
