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
package cn.edu.tsinghua.iginx.transform.data;

import cn.edu.tsinghua.iginx.constant.GlobalConstant;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogWriter extends ExportWriter {
  private static final Logger LOGGER = LoggerFactory.getLogger(LogWriter.class);

  private boolean hasWriteHeader;

  @Override
  public void write(BatchData batchData) {
    if (!hasWriteHeader) {
      Header header = batchData.getHeader();
      List<String> headerList = new ArrayList<>();
      if (header.hasKey()) {
        headerList.add(GlobalConstant.KEY_NAME);
      }
      header.getFields().forEach(field -> headerList.add(field.getFullName()));
      LOGGER.info(String.join(",", headerList));
      hasWriteHeader = true;
    }

    List<Row> rowList = batchData.getRowList();
    rowList.forEach(
        row -> {
          LOGGER.info(row.toCSVTypeString());
        });
  }
}
