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
package cn.edu.tsinghua.iginx.engine.shared.file.write;

import cn.edu.tsinghua.iginx.engine.shared.file.CSVFile;
import cn.edu.tsinghua.iginx.engine.shared.file.FileType;

public class ExportCsv implements ExportFile {

  private boolean isExportHeader;

  private final CSVFile csvFile;

  public ExportCsv(String filepath) {
    this.csvFile = new CSVFile(filepath);
    this.isExportHeader = false;
  }

  public boolean isExportHeader() {
    return isExportHeader;
  }

  public void setExportHeader(boolean exportHeader) {
    isExportHeader = exportHeader;
  }

  public CSVFile getCsvFile() {
    return csvFile;
  }

  public String getFilepath() {
    return csvFile.getFilepath();
  }

  @Override
  public FileType getType() {
    return FileType.CSV;
  }
}
