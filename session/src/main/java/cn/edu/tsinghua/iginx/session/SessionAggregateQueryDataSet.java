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
package cn.edu.tsinghua.iginx.session;

import cn.edu.tsinghua.iginx.thrift.AggregateQueryResp;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import cn.edu.tsinghua.iginx.utils.ByteUtils;
import java.util.List;

public class SessionAggregateQueryDataSet {

  private final AggregateType type;

  private List<String> paths;

  private final long[] keys;

  private final Object[] values;

  public SessionAggregateQueryDataSet(AggregateQueryResp resp, AggregateType type) {
    ByteUtils.DataSet dataSet = ByteUtils.getDataFromArrowData(resp.getQueryArrowData());
    this.keys = null; // 为了兼容 0.8.0 版本的接口行为
    this.paths = dataSet.getPaths();
    this.values = dataSet.getValues().get(0).toArray();
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
