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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import lombok.Data;

@Data
public class Metric {
  private String name;
  private Long startAbsolute;
  private Long endAbsolute;
  private Map<String, String> tags = new TreeMap<>();
  private List<Long> keys = new ArrayList<>();
  private List<String> values = new ArrayList<>();
  private Map<String, String> anno = new HashMap<>();
  private String annotation = null;

  public void addTag(String key, String value) {
    tags.put(key, value);
  }

  public void addKey(Long key) {
    keys.add(key);
  }

  public void addValue(String value) {
    values.add(value);
  }

  public void addAnno(String key, String value) {
    anno.put(key, value);
  }
}
