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

import cn.edu.tsinghua.iginx.thrift.DownsampleQueryResp;
import cn.edu.tsinghua.iginx.thrift.LastQueryResp;
import cn.edu.tsinghua.iginx.thrift.QueryDataResp;
import cn.edu.tsinghua.iginx.thrift.ShowColumnsResp;
import cn.edu.tsinghua.iginx.utils.ByteUtils;
import java.util.List;
import java.util.Map;

public class SessionQueryDataSet {

  private final long[] keys;
  private final List<String> paths;
  private final List<Map<String, String>> tagsList;
  private final List<List<Object>> values;

  public SessionQueryDataSet(LastQueryResp resp) {
    ByteUtils.DataSet dataSet = ByteUtils.getDataFromArrowData(resp.getQueryArrowData());
    this.keys = dataSet.getKeys();
    this.paths = dataSet.getPaths();
    this.tagsList = dataSet.getTagsList();
    this.values = dataSet.getValues();
  }

  public SessionQueryDataSet(ShowColumnsResp resp) {
    this.paths = resp.getPaths();
    this.keys = null;
    this.tagsList = null;
    this.values = null;
  }

  public SessionQueryDataSet(QueryDataResp resp) {
    ByteUtils.DataSet dataSet = ByteUtils.getDataFromArrowData(resp.getQueryArrowData());
    this.keys = dataSet.getKeys() == null ? new long[0] : dataSet.getKeys();
    this.paths = dataSet.getPaths();
    this.tagsList = dataSet.getTagsList();
    this.values = dataSet.getValues();
  }

  public SessionQueryDataSet(DownsampleQueryResp resp) {
    ByteUtils.DataSet dataSet = ByteUtils.getDataFromArrowData(resp.getQueryArrowData());
    this.keys = dataSet.getKeys() == null ? new long[0] : dataSet.getKeys();
    this.paths = dataSet.getPaths();
    this.tagsList = dataSet.getTagsList();
    this.values = dataSet.getValues();
  }

  public List<String> getPaths() {
    return paths;
  }

  public long[] getKeys() {
    return keys;
  }

  public List<Map<String, String>> getTagsList() {
    return tagsList;
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
