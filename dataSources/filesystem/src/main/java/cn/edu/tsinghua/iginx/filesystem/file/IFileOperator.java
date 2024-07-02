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
package cn.edu.tsinghua.iginx.filesystem.file;

import cn.edu.tsinghua.iginx.filesystem.file.entity.FileMeta;
import cn.edu.tsinghua.iginx.filesystem.query.entity.Record;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.io.File;
import java.io.IOException;
import java.util.List;

public interface IFileOperator {

  // read normal file by [startKey, endKey)
  byte[] readNormalFile(File file, long readPos, byte[] buffer) throws IOException;

  // read IGinX file by [startKey, endKey)
  List<Record> readIginxFile(File file, long startKey, long endKey, DataType dataType)
      throws IOException;

  void writeIginxFile(File file, List<Record> valList) throws IOException;

  File create(File file, FileMeta fileMeta) throws IOException;

  void delete(File file) throws IOException;

  void trimFile(File file, long begin, long end) throws IOException;

  FileMeta getFileMeta(File file) throws IOException;

  List<File> listFiles(File file);

  List<File> listFiles(File file, String prefix);
}
