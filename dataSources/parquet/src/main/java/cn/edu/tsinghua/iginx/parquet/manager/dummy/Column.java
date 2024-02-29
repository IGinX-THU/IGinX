/*
 * Copyright 2024 IGinX of Tsinghua University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.edu.tsinghua.iginx.parquet.manager.dummy;

import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.HashMap;
import java.util.Map;

@Deprecated
public class Column {

  private String pathName;

  private final String physicalPath;

  private final DataType type;

  private final Map<Long, Object> data = new HashMap<>();

  public Column(String pathName, String physicalPath, DataType type) {
    this.pathName = pathName;
    this.physicalPath = physicalPath;
    this.type = type;
  }

  public void putData(long time, Object value) {
    data.put(time, value);
  }

  public void putBatchData(Map<Long, Object> batchData) {
    data.putAll(batchData);
  }

  public void removeData(long time) {
    data.remove(time);
  }

  public String getPathName() {
    return pathName;
  }

  public String getPhysicalPath() {
    return physicalPath;
  }

  public DataType getType() {
    return type;
  }

  public Map<Long, Object> getData() {
    return data;
  }

  public void setPathName(String pathName) {
    this.pathName = pathName;
  }
}
