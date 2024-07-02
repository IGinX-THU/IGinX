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
package cn.edu.tsinghua.iginx.rest.bean;

import java.util.ArrayList;
import java.util.List;
import lombok.Data;

@Data
public class QueryResultDataset {
  private int size = 0;
  private int sampleSize = 0;
  private List<Long> keys = new ArrayList<>();
  private List<Object> values = new ArrayList<>();
  private List<List<Object>> valueLists = new ArrayList<>(); // 对应一个path的数据点序列值
  private List<List<Long>> keyLists = new ArrayList<>(); // 数据点的对应时间序列
  private List<String> paths = new ArrayList<>();
  private List<String> titles = new ArrayList<>();
  private List<String> descriptions = new ArrayList<>();
  private List<List<String>> categoryLists = new ArrayList<>();

  public void addPath(String path) {
    paths.add(path);
  }

  public void addTitle(String title) {
    titles.add(title);
  }

  public void addDescription(String description) {
    descriptions.add(description);
  }

  public void addCategory(List<String> categoryList) {
    categoryLists.add(categoryList);
  }

  private void addSize() {
    this.size++;
  }

  private void addKey(long key) {
    this.keys.add(key);
  }

  public void addValue(Object value) {
    this.values.add(value);
  }

  public void add(long key, Object value) {
    addKey(key);
    addValue(value);
    addSize();
  }

  public void addValueLists(List<Object> value) {
    valueLists.add(value);
  }

  public void addKeyLists(List<Long> keyList) {
    keyLists.add(keyList);
  }

  public void addPlus(
      String path,
      String description,
      List<String> categoryList,
      List<Object> valueList,
      List<Long> keyList) {
    paths.add(path);
    descriptions.add(description);
    categoryLists.add(categoryList);
    valueLists.add(valueList);
    keyLists.add(keyList);
  }
}
