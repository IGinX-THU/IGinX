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
import cn.edu.tsinghua.iginx.transform.exception.TransformException;
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

  public void process(BatchData batchData) throws TransformException {
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

    try {
      // Reload scripts between scheduled executions while keeping the interpreter thread-local.
      interpreter.exec("import importlib;importlib.reload(" + moduleName + ")");
      String obj = (moduleName + className).replace(".", "a");
      interpreter.exec(String.format("%s = %s.%s()", obj, moduleName, className));
      Object result = interpreter.invokeMethod(obj, UDF_FUNC, data);
      PemjaReader reader = new PemjaReader(result, config.getBatchSize());
      while (reader.hasNextBatch()) {
        BatchData nextBatchData = reader.loadNextBatch();
        writer.writeBatch(nextBatchData);
      }
    } catch (ReadBatchException e) {
      throw new TransformException(
          "Invalid output from Python transformer '" + identifier + "': " + e.getMessage(), e);
    } catch (TransformException e) {
      throw e;
    } catch (Exception e) {
      String inputState = batchData.isEmpty() ? "empty" : "non-empty";
      throw new TransformException(
          "Python transformer '" + identifier + "' failed with " + inputState + " input.", e);
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
