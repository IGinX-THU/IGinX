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
package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.engine.shared.data.write.RawData;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RawDataType;
import cn.edu.tsinghua.iginx.sql.SQLConstant;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import java.util.*;

public class InsertStatement extends DataStatement {

  private final RawDataType rawDataType;

  private String prefixPath;
  private List<String> paths; // full paths, prefixed by the above prefixPath
  private Map<String, String> globalTags;
  private List<Map<String, String>> tagsList;
  private List<Long> keys;
  private Object[] values; // 按列组织，每个元数是一列
  private List<DataType> types;
  private List<Bitmap> bitmaps;

  public InsertStatement(RawDataType rawDataType) {
    this.statementType = StatementType.INSERT;
    this.rawDataType = rawDataType;
    this.paths = new ArrayList<>();
    this.types = new ArrayList<>();
    this.bitmaps = new ArrayList<>();
    this.tagsList = new ArrayList<>();
  }

  public InsertStatement(
      RawDataType rawDataType,
      List<String> paths,
      List<Long> keys,
      Object[] values,
      List<DataType> types,
      List<Bitmap> bitmaps,
      List<Map<String, String>> tagsList) {
    this.statementType = StatementType.INSERT;
    this.rawDataType = rawDataType;
    this.paths = paths;
    this.keys = keys;
    this.values = values;
    this.types = types;
    this.bitmaps = bitmaps;
    this.tagsList = tagsList;
  }

  public String getPrefixPath() {
    return prefixPath;
  }

  public void setPrefixPath(String prefixPath) {
    this.prefixPath = prefixPath;
  }

  public List<String> getPaths() {
    return paths;
  }

  public void setPaths(List<String> paths) {
    this.paths = paths;
  }

  public void setPath(String path) {
    setPath(path, null);
  }

  public void setPath(String path, Map<String, String> tags) {
    this.paths.add(prefixPath + SQLConstant.DOT + path);
    this.tagsList.add(tags);
  }

  public Map<String, String> getGlobalTags() {
    return globalTags;
  }

  public void setGlobalTags(Map<String, String> globalTags) {
    this.globalTags = globalTags;
  }

  public boolean hasGlobalTags() {
    return this.globalTags != null;
  }

  public List<Map<String, String>> getTagsList() {
    return tagsList;
  }

  public void setTagsList(List<Map<String, String>> tagsList) {
    this.tagsList = tagsList;
  }

  public List<Long> getKeys() {
    return keys;
  }

  public void setKeys(List<Long> keys) {
    this.keys = keys;
  }

  public Object[] getValues() {
    return values;
  }

  public void setValues(Object[][] values) {
    this.values = values;
  }

  public List<DataType> getTypes() {
    return types;
  }

  public void setTypes(List<DataType> types) {
    this.types = types;
  }

  public List<Bitmap> getBitmaps() {
    return bitmaps;
  }

  public void setBitmaps(List<Bitmap> bitmaps) {
    this.bitmaps = bitmaps;
  }

  public long getStartKey() {
    return keys.get(0);
  }

  public long getEndKey() {
    return keys.get(keys.size() - 1);
  }

  public void sortData() {
    Integer[] index = new Integer[keys.size()];
    for (int i = 0; i < keys.size(); i++) {
      index[i] = i;
    }
    Arrays.sort(index, Comparator.comparingLong(keys::get));
    Collections.sort(keys);
    for (int i = 0; i < values.length; i++) {
      Object[] tmpValues = new Object[index.length];
      for (int j = 0; j < index.length; j++) {
        tmpValues[j] = ((Object[]) values[i])[index[j]];
      }
      values[i] = tmpValues;
    }

    index = new Integer[paths.size()];
    for (int i = 0; i < paths.size(); i++) {
      index[i] = i;
    }
    Arrays.sort(index, Comparator.comparing(paths::get));
    Collections.sort(paths);
    Object[] sortedValuesList = new Object[values.length];
    List<DataType> sortedDataTypeList = new ArrayList<>();
    List<Map<String, String>> sortedTagsList = new ArrayList<>();
    for (int i = 0; i < values.length; i++) {
      sortedValuesList[i] = values[index[i]];
      sortedDataTypeList.add(types.get(index[i]));
      if (!hasGlobalTags()) {
        sortedTagsList.add(tagsList.get(index[i]));
      }
    }

    for (int i = 0; i < sortedValuesList.length; i++) {
      Object[] values = (Object[]) sortedValuesList[i];
      Bitmap bitmap = new Bitmap(keys.size());
      for (int j = 0; j < keys.size(); j++) {
        if (values[j] != null) {
          bitmap.mark(j);
        }
      }
      bitmaps.add(bitmap);
    }

    values = sortedValuesList;
    types = sortedDataTypeList;
    tagsList = sortedTagsList;
  }

  public RawData getRawData() {
    List<Map<String, String>> tagsList = this.tagsList;
    if (globalTags != null) {
      for (int i = 0; i < paths.size(); i++) {
        tagsList.add(globalTags);
      }
    }
    return new RawData(paths, tagsList, keys, values, types, bitmaps, rawDataType);
  }
}
