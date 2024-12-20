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
package cn.edu.tsinghua.iginx.transform.driver;

import static cn.edu.tsinghua.iginx.transform.utils.Constants.UDF_FUNC;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.constant.GlobalConstant;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.transform.api.Writer;
import cn.edu.tsinghua.iginx.transform.data.BatchData;
import cn.edu.tsinghua.iginx.transform.data.PemjaReader;
import cn.edu.tsinghua.iginx.transform.exception.ReadBatchException;
import cn.edu.tsinghua.iginx.transform.exception.WriteBatchException;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pemja.core.PythonInterpreter;

public class PemjaWorker {
  private static final Logger LOGGER = LoggerFactory.getLogger(PemjaWorker.class);

  private final String identifier;

  private final String moduleName;

  private final String className;

  private final PythonInterpreter interpreter;

  private final Writer writer;

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  public PemjaWorker(
      String identifier,
      String moduleName,
      String className,
      PythonInterpreter interpreter,
      Writer writer) {
    this.identifier = identifier;
    this.moduleName = moduleName;
    this.className = className;
    this.interpreter = interpreter;
    this.writer = writer;
  }

  public void process(BatchData batchData) {
    List<List<Object>> data = new ArrayList<>();

    List<Object> headerRow = new ArrayList<>();
    Header header = batchData.getHeader();
    if (header.hasKey()) {
      headerRow.add(GlobalConstant.KEY_NAME);
    }
    header.getFields().forEach(field -> headerRow.add(field.getFullName()));
    data.add(headerRow);

    batchData
        .getRowList()
        .forEach(
            row -> {
              if (row.getHeader().hasKey()) {
                List<Object> rowData = new ArrayList<>();
                rowData.add(row.getKey());
                rowData.addAll(Arrays.asList(row.getValues()));
                data.add(rowData);
              } else {
                data.add(Arrays.asList(row.getValues()));
              }
            });

    // no need to use a new thread because the whole job is running on a seperated
    // thread(scheduler).
    // reload module in case of script modification
    interpreter.exec("import importlib;importlib.reload(" + moduleName + ")");
    // use unique name in shared interpreter
    String obj = (moduleName + className).replace(".", "a");
    interpreter.exec(String.format("%s = %s.%s()", obj, moduleName, className));
    List<Object> res = (List<Object>) interpreter.invokeMethod(obj, UDF_FUNC, data);

    try {
      PemjaReader reader = new PemjaReader(res, config.getBatchSize());
      while (reader.hasNextBatch()) {
        BatchData nextBatchData = reader.loadNextBatch();
        writer.writeBatch(nextBatchData);
      }
    } catch (WriteBatchException e) {
      LOGGER.error("PemjaWorker identifier={} fail to writer data.", identifier, e);
    } catch (ReadBatchException e) {
      LOGGER.error("Failed to read data from python transformer.", e);
    }
  }

  public String getIdentifier() {
    return identifier;
  }

  public Writer getWriter() {
    return writer;
  }

  /** leave for future */
  public void close() {}
}
