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

import static cn.edu.tsinghua.iginx.utils.ByteUtils.getLongArrayFromByteBuffer;

import cn.edu.tsinghua.iginx.thrift.AggregateQueryResp;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import cn.edu.tsinghua.iginx.utils.ByteUtils;
import java.util.List;

public class SessionAggregateQueryDataSet {

  private final AggregateType type;

  private List<String> paths;

  private long[] keys;

  private final Object[] values;

  public SessionAggregateQueryDataSet(AggregateQueryResp resp, AggregateType type) {
    this.paths = resp.getPaths();
    if (resp.keys != null) {
      this.keys = getLongArrayFromByteBuffer(resp.keys);
    }
    this.values = ByteUtils.getValuesByDataType(resp.valuesList, resp.dataTypeList);
    this.type = type;
  }

  public List<String> getPaths() {
    return paths;
  }

  public void setPaths(List<String> paths) {
    this.paths = paths;
  }

  public long[] getKeys() {
    return keys;
  }

  public Object[] getValues() {
    return values;
  }

  public void print() {
    System.out.println("Start to Print ResultSets:");
    if (keys == null) {
      for (String path : paths) {
        System.out.print(path + "\t");
      }
      System.out.println();
      for (Object value : values) {
        if (value instanceof byte[]) {
          System.out.print(new String((byte[]) value) + "\t");
        } else {
          System.out.print(value + "\t");
        }
      }
      System.out.println();
    } else {
      for (int i = 0; i < keys.length; i++) {
        System.out.print("Time\t");
        System.out.print(paths.get(i) + "\t");
        System.out.println();
        System.out.print(keys[i] + "\t");
        if (values[i] instanceof byte[]) {
          System.out.print(new String((byte[]) values[i]) + "\t");
        } else {
          System.out.print(values[i] + "\t");
        }
        System.out.println();
      }
    }
    System.out.println("Printing ResultSets Finished.");
  }
}
