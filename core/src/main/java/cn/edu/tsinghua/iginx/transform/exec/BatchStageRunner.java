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
package cn.edu.tsinghua.iginx.transform.exec;

import cn.edu.tsinghua.iginx.transform.api.Runner;
import cn.edu.tsinghua.iginx.transform.api.Writer;
import cn.edu.tsinghua.iginx.transform.data.*;
import cn.edu.tsinghua.iginx.transform.driver.PemjaDriver;
import cn.edu.tsinghua.iginx.transform.driver.PemjaWorker;
import cn.edu.tsinghua.iginx.transform.exception.CreateWorkerException;
import cn.edu.tsinghua.iginx.transform.exception.TransformException;
import cn.edu.tsinghua.iginx.transform.exception.WriteBatchException;
import cn.edu.tsinghua.iginx.transform.pojo.BatchStage;
import cn.edu.tsinghua.iginx.transform.pojo.PythonTask;
import cn.edu.tsinghua.iginx.transform.pojo.Task;
import cn.edu.tsinghua.iginx.transform.utils.Mutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchStageRunner implements Runner {
  private static final Logger LOGGER = LoggerFactory.getLogger(BatchStageRunner.class);

  private final BatchStage batchStage;

  private final Mutex mutex;

  private Writer writer;

  private PemjaWorker pemjaWorker;

  private final PemjaDriver driver = PemjaDriver.getInstance();

  public BatchStageRunner(BatchStage batchStage) {
    this.batchStage = batchStage;
    this.writer = batchStage.getExportWriter();
    this.mutex = ((ExportWriter) writer).getMutex();
  }

  @Override
  public void start() throws TransformException {
    Task task = batchStage.getTask();
    if (task.isPythonTask()) {
      pemjaWorker = driver.createWorker((PythonTask) task, writer);
    } else {
      LOGGER.error("Batch task must be python task.");
      throw new CreateWorkerException("Only python task can create worker.");
    }
    writer = new PemjaWriter(pemjaWorker);
  }

  @Override
  public void run() throws WriteBatchException {
    CollectionWriter collectionWriter =
        (CollectionWriter) batchStage.getBeforeStage().getExportWriter();
    BatchData batchData = collectionWriter.getCollectedData();

    mutex.lock();
    writer.writeBatch(batchData);

    // wait for py work finish writing.
    mutex.lock();
  }

  @Override
  public void close() {}
}
