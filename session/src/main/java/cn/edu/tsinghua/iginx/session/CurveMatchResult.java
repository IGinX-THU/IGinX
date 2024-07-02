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
package cn.edu.tsinghua.iginx.session;

public class CurveMatchResult {

  private final long matchedTimestamp;
  private final String matchedPath;

  public CurveMatchResult(long matchedTimestamp, String matchedPath) {
    this.matchedTimestamp = matchedTimestamp;
    this.matchedPath = matchedPath;
  }

  public long getMatchedTimestamp() {
    return matchedTimestamp;
  }

  public String getMatchedPath() {
    return matchedPath;
  }

  @Override
  public String toString() {
    return "CurveMatchResult{"
        + "matchedTimestamp="
        + matchedTimestamp
        + ", matchedPath='"
        + matchedPath
        + '\''
        + '}';
  }
}
