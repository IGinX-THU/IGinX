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
package cn.edu.tsinghua.iginx.filesystem.struct.legacy.filesystem.file.entity;

import static cn.edu.tsinghua.iginx.filesystem.struct.legacy.filesystem.shared.Constant.MAGIC_NUMBER;

import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.HashMap;
import java.util.Map;

public class FileMeta {

  private byte[] magicNumber = MAGIC_NUMBER;

  private DataType dataType;

  private Map<String, String> tags = new HashMap<>();

  public FileMeta() {}

  public FileMeta(DataType dataType, Map<String, String> tags) {
    this.dataType = dataType;
    if (tags != null) {
      this.tags = tags;
    }
  }

  public DataType getDataType() {
    return dataType;
  }

  public void setDataType(DataType dataType) {
    this.dataType = dataType;
  }

  public Map<String, String> getTags() {
    return tags;
  }

  public void setTags(Map<String, String> tags) {
    this.tags = tags;
  }

  public byte[] getMagicNumber() {
    return magicNumber;
  }

  public void setMagicNumber(byte[] magicNumber) {
    this.magicNumber = magicNumber;
  }
}
