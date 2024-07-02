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
package cn.edu.tsinghua.iginx.shared;

import cn.edu.tsinghua.iginx.conf.Constants;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.DataArea;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RawData;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RawDataType;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RowDataView;
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;
import cn.edu.tsinghua.iginx.metadata.entity.*;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.StorageEngineType;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockClassGenerator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MockClassGenerator.class);

  public static DataView genRowDataViewNoKey(
      long keyStart,
      List<String> pathList,
      List<Map<String, String>> tagsList,
      List<DataType> dataTypeList,
      Object[] valuesList) {
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
    for (int i = 0; i < valuesList.length; i++) {
      Object[] values = new Object[index.length];
      for (int j = 0; j < index.length; j++) {
        values[j] = ((Object[]) valuesList[i])[index[j]];
      }
      valuesList[i] = values;
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
    List<Long> keys = new ArrayList<>();
    long keyIndex = keyStart;
    for (Object o : valuesList) {
      Object[] values = (Object[]) o;
      keys.add(keyIndex++);
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
            keys,
            valuesList,
            sortedDataTypeList,
            bitmapList,
            RawDataType.Row);

    return new RowDataView(rawData, 0, sortedPaths.size(), 0, valuesList.length);
  }

  public static FragmentMeta genFragmentMeta() {
    return new FragmentMeta(null, null, 0, 0);
  }

  public static FragmentSource genFragmentSource() {
    return new FragmentSource(genFragmentMeta());
  }

  public static KeyInterval genKeyInterval() {
    return new KeyInterval(0, Long.MAX_VALUE);
  }

  public static DataArea genDataArea() {
    return new DataArea("unit0000000000", genKeyInterval());
  }

  public static StorageEngineMeta genStorageEngineMetaFromConf(String storageEngineString) {
    if (storageEngineString.isEmpty()) {
      return null;
    }
    String[] storageEngineParts = storageEngineString.split("#");
    String ip = storageEngineParts[0];
    int port = -1;
    if (!storageEngineParts[1].isEmpty()) {
      port = Integer.parseInt(storageEngineParts[1]);
    }
    String storageEngine = storageEngineParts[2];
    Map<String, String> extraParams = new HashMap<>();
    String[] KAndV;
    for (int j = 3; j < storageEngineParts.length; j++) {
      if (storageEngineParts[j].contains("\"")) {
        KAndV = storageEngineParts[j].split("\"");
        extraParams.put(KAndV[0].substring(0, KAndV[0].length() - 1), KAndV[1]);
      } else {
        KAndV = storageEngineParts[j].split("=");
        extraParams.put(KAndV[0], KAndV[1]);
      }
    }
    boolean hasData = Boolean.parseBoolean(extraParams.getOrDefault(Constants.HAS_DATA, "false"));
    String dataPrefix = null;
    if (hasData && extraParams.containsKey(Constants.DATA_PREFIX)) {
      dataPrefix = extraParams.get(Constants.DATA_PREFIX);
    }
    boolean readOnly =
        Boolean.parseBoolean(extraParams.getOrDefault(Constants.IS_READ_ONLY, "false"));
    String schemaPrefix = extraParams.get(Constants.SCHEMA_PREFIX);

    StorageEngineMeta storage =
        new StorageEngineMeta(
            0,
            ip,
            port,
            hasData,
            dataPrefix,
            schemaPrefix,
            readOnly,
            extraParams,
            StorageEngineType.valueOf(storageEngine.toLowerCase()),
            0);
    return storage;
  }
}
