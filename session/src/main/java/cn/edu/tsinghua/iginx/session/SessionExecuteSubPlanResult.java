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

import static cn.edu.tsinghua.iginx.utils.ByteUtils.getLongArrayFromByteBuffer;
import static cn.edu.tsinghua.iginx.utils.ByteUtils.getValuesFromBufferAndBitmaps;

import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.ExecuteSubPlanResp;
import java.util.ArrayList;
import java.util.List;

public class SessionExecuteSubPlanResult {

  private final long[] keys;
  private final List<String> paths;
  private final List<List<Object>> values;
  private final List<DataType> dataTypeList;

  public SessionExecuteSubPlanResult(ExecuteSubPlanResp resp) {
    this.paths = resp.getPaths();
    this.dataTypeList = resp.getDataTypeList();

    if (resp.getKeys() != null) {
      this.keys = getLongArrayFromByteBuffer(resp.keys);
    } else {
      this.keys = null;
    }

    // parse values
    if (resp.getQueryDataSet() != null) {
      this.values =
          getValuesFromBufferAndBitmaps(
              resp.dataTypeList, resp.queryDataSet.valuesList, resp.queryDataSet.bitmapList);
    } else {
      this.values = new ArrayList<>();
    }
  }

  public long[] getKeys() {
    return keys;
  }

  public List<String> getPaths() {
    return paths;
  }

  public List<List<Object>> getValues() {
    return values;
  }

  public List<DataType> getDataTypeList() {
    return dataTypeList;
  }
}
