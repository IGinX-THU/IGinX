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

import static cn.edu.tsinghua.iginx.utils.ByteUtils.*;

import cn.edu.tsinghua.iginx.thrift.DownsampleQueryResp;
import cn.edu.tsinghua.iginx.thrift.LastQueryResp;
import cn.edu.tsinghua.iginx.thrift.QueryDataResp;
import cn.edu.tsinghua.iginx.thrift.ShowColumnsResp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SessionQueryDataSet {

  private final long[] keys;
  private List<String> paths;
  private List<Map<String, String>> tagsList;
  private List<List<Object>> values;

  public SessionQueryDataSet(LastQueryResp resp) {
    this.paths = resp.getPaths();
    this.tagsList = resp.getTagsList();
    this.keys = getLongArrayFromByteBuffer(resp.queryDataSet.keys);
    this.values =
        getValuesFromBufferAndBitmaps(
            resp.dataTypeList, resp.queryDataSet.valuesList, resp.queryDataSet.bitmapList);
  }

  public SessionQueryDataSet(ShowColumnsResp resp) {
    this.paths = resp.getPaths();
    this.keys = null;
  }

  public SessionQueryDataSet(QueryDataResp resp) {
    this.paths = resp.getPaths();
    this.tagsList = resp.getTagsList();
    this.keys = getLongArrayFromByteBuffer(resp.queryDataSet.keys);
    this.values =
        getValuesFromBufferAndBitmaps(
            resp.dataTypeList, resp.queryDataSet.valuesList, resp.queryDataSet.bitmapList);
  }

  public SessionQueryDataSet(DownsampleQueryResp resp) {
    this.paths = resp.getPaths();
    this.tagsList = resp.getTagsList();
    if (resp.queryDataSet != null) {
      this.keys = getLongArrayFromByteBuffer(resp.queryDataSet.keys);
      this.values =
          getValuesFromBufferAndBitmaps(
              resp.dataTypeList, resp.queryDataSet.valuesList, resp.queryDataSet.bitmapList);
    } else {
      this.keys = new long[0];
      values = new ArrayList<>();
    }
    if (this.paths == null) {
      this.paths = new ArrayList<>();
    }
  }

  public List<String> getPaths() {
    return paths;
  }

  public long[] getKeys() {
    return keys;
  }

  public List<List<Object>> getValues() {
    return values;
  }

  public void print() {
    System.out.println("Start to Print ResultSets:");
    System.out.print("Time\t");
    for (int i = 0; i < paths.size(); i++) {
      System.out.print(paths.get(i) + "\t");
    }
    System.out.println();

    for (int i = 0; i < keys.length; i++) {
      System.out.print(keys[i] + "\t");
      for (int j = 0; j < paths.size(); j++) {
        if (values.get(i).get(j) instanceof byte[]) {
          System.out.print(new String((byte[]) values.get(i).get(j)) + "\t");
        } else {
          System.out.print(values.get(i).get(j) + "\t");
        }
      }
      System.out.println();
    }
    System.out.println("Printing ResultSets Finished.");
  }
}
