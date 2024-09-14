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
package cn.edu.tsinghua.iginx.filestore.test;

import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RawData;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RawDataType;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RowDataView;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import java.util.*;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataViewGenerator {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataViewGenerator.class);

  public static DataView genRowDataViewNoKey(
      long keyStart,
      List<String> pathList,
      List<Map<String, String>> tagsList,
      List<DataType> dataTypeList,
      List<Object[]> valuesList) {
    List<Long> keyList = new ArrayList<>();
    for (int i = 0; i < valuesList.size(); i++) {
      keyList.add(keyStart + i);
    }
    return genRowDataView(pathList, tagsList, dataTypeList, keyList, valuesList);
  }

  public static DataView genRowDataView(
      List<String> pathList,
      List<DataType> dataTypeList,
      List<Long> keyList,
      List<List<Object>> valuesList) {
    List<Object[]> valueArrayList =
        valuesList.stream().map(List::toArray).collect(Collectors.toList());
    return genRowDataView(pathList, null, dataTypeList, keyList, valueArrayList);
  }

  public static DataView genRowDataView(
      List<String> pathList,
      List<Map<String, String>> tagsList,
      List<DataType> dataTypeList,
      List<Long> keyList,
      List<Object[]> valuesList) {
    // sort path by dictionary
    List<String> sortedPaths = new ArrayList<>(pathList);
    Integer[] index = new Integer[sortedPaths.size()];
    for (int i = 0; i < sortedPaths.size(); i++) {
      index[i] = i;
    }
    Arrays.sort(index, Comparator.comparing(sortedPaths::get));
    Collections.sort(sortedPaths);
    List<DataType> sortedDataTypeList = new ArrayList<>();
    List<Map<String, String>> sortedTagsList = new ArrayList<>();
    for (int i = 0; i < valuesList.size(); i++) {
      Object[] values = new Object[index.length];
      for (int j = 0; j < index.length; j++) {
        values[j] = (valuesList.get(i))[index[j]];
      }
      valuesList.set(i, values);
    }
    for (Integer i : index) {
      sortedDataTypeList.add(dataTypeList.get(i));
    }
    if (tagsList != null) {
      for (Integer i : index) {
        sortedTagsList.add(tagsList.get(i));
      }
    }

    // generate bitmaps and key
    List<Bitmap> bitmapList = new ArrayList<>();
    for (Object[] values : valuesList) {
      if (values.length != pathList.size()) {
        LOGGER.error("The sizes of paths and the element of valuesList should be equal.");
        return null;
      }
      Bitmap bitmap = new Bitmap(values.length);
      for (int j = 0; j < values.length; j++) {
        if (values[j] != null) {
          bitmap.mark(j);
        }
      }
      bitmapList.add(bitmap);
    }

    RawData rawData =
        new RawData(
            sortedPaths,
            sortedTagsList,
            keyList,
            valuesList.toArray(),
            sortedDataTypeList,
            bitmapList,
            RawDataType.Row);

    return new RowDataView(rawData, 0, sortedPaths.size(), 0, valuesList.size());
  }
}
