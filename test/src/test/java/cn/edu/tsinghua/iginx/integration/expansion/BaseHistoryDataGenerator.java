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
package cn.edu.tsinghua.iginx.integration.expansion;

import static cn.edu.tsinghua.iginx.integration.expansion.constant.Constant.*;

import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public abstract class BaseHistoryDataGenerator {

  public BaseHistoryDataGenerator() {}

  @Test
  public void oriHasDataExpHasData() {
    // 向扩容节点写入边界数据，以指定分片范围
    writeInitDataToDummy(expPort);
    // 向原始节点写入边界数据，以指定分片范围
    writeInitDataToDummy(oriPort);
    // 向原始节点写入历史数据
    writeHistoryDataToOri();
    // 向扩容节点写入历史数据
    writeHistoryDataToExp();
    // 向只读节点写入历史数据
    writeHistoryDataToReadOnly();
    // 某些数据库有特殊历史数据写入需要，则实现
    writeSpecialHistoryData();
  }

  @Test
  public void oriHasDataExpNoData() {
    // 向原始节点写入边界数据，以指定分片范围
    writeInitDataToDummy(oriPort);
    // 向原始节点写入历史数据
    writeHistoryDataToOri();
  }

  @Test
  public void oriNoDataExpHasData() {
    // 向扩容节点写入边界数据，以指定分片范围
    writeInitDataToDummy(expPort);
    // 向扩容节点写入历史数据
    writeHistoryDataToExp();
  }

  @Test
  public void oriNoDataExpNoData() {}

  public void writeSpecialHistoryData() {}

  public void writeInitDataToDummy(int port) {
    writeHistoryData(port, INIT_PATH_LIST, INIT_DATA_TYPE_LIST, INIT_KEYS_LIST, INIT_VALUES_LIST);
  }

  public void writeExtendDummyData() {
    writeExtendedHistoryDataToOri();
    writeExtendedHistoryDataToExp();
    writeExtendedHistoryDataToReadOnly();
  }

  public void writeHistoryDataToOri() {
    writeHistoryData(oriPort, ORI_PATH_LIST, ORI_DATA_TYPE_LIST, ORI_VALUES_LIST);
  }

  // write dummy data that contains key & columns that are not in initial range
  public void writeExtendedHistoryDataToOri() {
    writeHistoryData(oriPort, ORI_EXTEND_PATH_LIST, ORI_DATA_TYPE_LIST, ORI_VALUES_LIST);
    writeHistoryData(
        oriPort,
        ORI_PATH_LIST,
        ORI_DATA_TYPE_LIST,
        new ArrayList<>(Collections.singletonList(999999L)),
        ORI_EXTEND_VALUES_LIST);
  }

  public void writeHistoryDataToExp() {
    writeHistoryData(expPort, EXP_PATH_LIST, EXP_DATA_TYPE_LIST, EXP_VALUES_LIST);
  }

  public void writeExtendedHistoryDataToExp() {
    writeHistoryData(expPort, EXP_EXTEND_PATH_LIST, EXP_DATA_TYPE_LIST, EXP_VALUES_LIST);
    writeHistoryData(
        expPort,
        EXP_PATH_LIST,
        EXP_DATA_TYPE_LIST,
        new ArrayList<>(Collections.singletonList(999999L)),
        EXP_EXTEND_VALUES_LIST);
  }

  public void writeHistoryDataToReadOnly() {
    writeHistoryData(
        readOnlyPort, READ_ONLY_PATH_LIST, READ_ONLY_DATA_TYPE_LIST, READ_ONLY_VALUES_LIST);
  }

  public void writeExtendedHistoryDataToReadOnly() {
    writeHistoryData(
        readOnlyPort, READ_ONLY_EXTEND_PATH_LIST, READ_ONLY_DATA_TYPE_LIST, READ_ONLY_VALUES_LIST);
    writeHistoryData(
        readOnlyPort,
        READ_ONLY_PATH_LIST,
        READ_ONLY_DATA_TYPE_LIST,
        new ArrayList<>(Collections.singletonList(999999L)),
        READ_ONLY_EXTEND_VALUES_LIST);
  }

  public abstract void writeHistoryData(
      int port, List<String> pathList, List<DataType> dataTypeList, List<List<Object>> valuesList);

  public abstract void writeHistoryData(
      int port,
      List<String> pathList,
      List<DataType> dataTypeList,
      List<Long> keyList,
      List<List<Object>> valuesList);

  @Test
  public void clearHistoryData() {
    clearHistoryDataForGivenPort(oriPort);
    clearHistoryDataForGivenPort(expPort);
    clearHistoryDataForGivenPort(readOnlyPort);
  }

  public abstract void clearHistoryDataForGivenPort(int port);
}
