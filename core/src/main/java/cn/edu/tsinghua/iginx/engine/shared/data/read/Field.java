/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package cn.edu.tsinghua.iginx.engine.shared.data.read;

import cn.edu.tsinghua.iginx.constant.GlobalConstant;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.TagKVUtils;
import java.util.*;

public final class Field {

  public static final Field KEY = new Field();

  private final String name;

  private final String fullName;

  private final Map<String, String> tags;

  private final DataType type;

  public Field() {
    this(GlobalConstant.KEY_NAME, DataType.LONG, Collections.emptyMap());
  }

  public Field(String name, DataType type) {
    this(name, type, Collections.emptyMap());
  }

  public Field(String name, DataType type, Map<String, String> tags) {
    this(name, TagKVUtils.toFullName(name, tags), type, tags);
  }

  public Field(String name, String fullName, DataType type) {
    this(name, fullName, type, Collections.emptyMap());
  }

  private Field(String name, String fullName, DataType type, Map<String, String> tags) {
    this.name = Objects.requireNonNull(name);
    this.fullName = Objects.requireNonNull(fullName);
    this.type = Objects.requireNonNull(type);
    this.tags = Optional.ofNullable(tags).orElse(Collections.emptyMap());
  }

  public String getName() {
    return name;
  }

  public String getFullName() {
    return fullName;
  }

  public DataType getType() {
    return type;
  }

  public Map<String, String> getTags() {
    return tags;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", Field.class.getSimpleName() + "{", "}")
        .add("name='" + name + "'")
        .add("fullName='" + fullName + "'")
        .add("tags=" + tags)
        .add("type=" + type)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Field that = (Field) o;
    return Objects.equals(name, that.name)
        && Objects.equals(fullName, that.fullName)
        && type == that.type
        && Objects.equals(tags, that.tags);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, fullName, type, tags);
  }
}
