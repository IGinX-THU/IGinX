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
package cn.edu.tsinghua.iginx.transform.exec;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.ContextBuilder;
import cn.edu.tsinghua.iginx.engine.StatementExecutor;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.thrift.ExecuteStatementReq;
import cn.edu.tsinghua.iginx.transform.api.Reader;
import cn.edu.tsinghua.iginx.transform.api.Runner;
import cn.edu.tsinghua.iginx.transform.api.Writer;
import cn.edu.tsinghua.iginx.transform.data.*;
import cn.edu.tsinghua.iginx.transform.driver.PemjaDriver;
import cn.edu.tsinghua.iginx.transform.driver.PemjaWorker;
import cn.edu.tsinghua.iginx.transform.exception.TransformException;
import cn.edu.tsinghua.iginx.transform.exception.WriteBatchException;
import cn.edu.tsinghua.iginx.transform.pojo.IginXTask;
import cn.edu.tsinghua.iginx.transform.pojo.PythonTask;
import cn.edu.tsinghua.iginx.transform.pojo.StreamStage;
import cn.edu.tsinghua.iginx.transform.pojo.Task;
import cn.edu.tsinghua.iginx.transform.utils.Mutex;
import cn.edu.tsinghua.iginx.utils.RpcUtils;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamStageRunner implements Runner {
  @SuppressWarnings("unused")
  private static final Logger LOGGER = LoggerFactory.getLogger(StreamStageRunner.class);

  private final StreamStage streamStage;

  private final int batchSize;

  private final Mutex mutex;

  private Writer writer;

  private Reader reader;

  private final List<PemjaWorker> pemjaWorkerList;

  private final PemjaDriver driver = PemjaDriver.getInstance();

  private final StatementExecutor executor = StatementExecutor.getInstance();

  private final ContextBuilder contextBuilder = ContextBuilder.getInstance();

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private final List<RequestContext> ctxToBeClosed = new ArrayList<>();

  public StreamStageRunner(StreamStage stage) {
    this.streamStage = stage;
    this.batchSize = config.getBatchSize();
    this.pemjaWorkerList = new ArrayList<>();
    this.writer = streamStage.getExportWriter();
    this.mutex = ((ExportWriter) writer).getMutex();
  }

  @Override
  public void start() throws TransformException {
    if (streamStage.isStartWithIginX()) {
      IginXTask firstTask = (IginXTask) streamStage.getTaskList().get(0);
      reader = getBatchStreamReader(streamStage.getSessionId(), firstTask.getSqlList());
    } else {
      CollectionWriter collectionWriter =
          (CollectionWriter) streamStage.getBeforeStage().getExportWriter();
      reader = new SplitReader(collectionWriter.getCollectedData(), batchSize);
    }

    List<Task> taskList = streamStage.getTaskList();
    pemjaWorkerList.clear();
    this.writer = streamStage.getExportWriter();
    for (int i = taskList.size() - 1; i >= 0; i--) {
      Task task = taskList.get(i);
      if (task.isPythonTask()) {
        PemjaWorker pemjaWorker = driver.createWorker((PythonTask) task, writer);
        pemjaWorkerList.add(0, pemjaWorker);
        writer = new PemjaWriter(pemjaWorker);
      }
    }
  }

  private BatchStreamReader getBatchStreamReader(long sessionId, List<String> sqlList)
      throws TransformException {
    for (int i = 0; i < sqlList.size() - 1; i++) {
      ExecuteStatementReq req = new ExecuteStatementReq(sessionId, sqlList.get(i));
      RequestContext context = contextBuilder.build(req);
      executor.execute(context);
      if (context.getResult().getStatus().code != RpcUtils.SUCCESS.code) {
        if (!context.getWarningMsg().contains("overlapped keys")) {
          throw new TransformException(
              "Unexpected error occurred during iginx task stage: "
                  + context.getResult().getStatus().getMessage());
        } else {
          // warn about overlapped keys but continue job
          LOGGER.warn(
              "Result of IGinX query task in transform job contains overlapped keys! Query: {}",
              sqlList.get(i));
        }
      }
    }

    ExecuteStatementReq req = new ExecuteStatementReq(sessionId, sqlList.get(sqlList.size() - 1));
    RequestContext context = contextBuilder.build(req);
    executor.execute(context, false);
    ctxToBeClosed.add(context);
    if (context.getResult().getStatus().code != RpcUtils.SUCCESS.code) {
      if (!context.getWarningMsg().contains("overlapped keys")) {
        throw new TransformException(
            "Unexpected error occurred during iginx task stage: "
                + context.getResult().getStatus().getMessage());
      } else {
        // warn about overlapped keys but continue job
        LOGGER.warn(
            "Result of IGinX query task in transform job contains overlapped keys! Query: {}",
            sqlList.get(sqlList.size() - 1));
      }
    }
    return new BatchStreamReader(context.getResult().getBatchStream());
  }

  @Override
  public void run() throws WriteBatchException {
    while (reader.hasNextBatch()) {
      mutex.lock();
      BatchData batchData = reader.loadNextBatch();
      writer.writeBatch(batchData);
    }

    // wait for last batch finished.
    mutex.lock();

    // unlock for further scheduled runs
    mutex.unlock();
    writer.reset();
  }

  @Override
  public void close() {
    if (reader != null) {
      reader.close();
    }
    if (!pemjaWorkerList.isEmpty()) {
      pemjaWorkerList.forEach(PemjaWorker::close);
    }
    try {
      for (RequestContext requestContext : ctxToBeClosed) {
        requestContext.closeResources();
      }
    } catch (Exception e) {
      LOGGER.error("Cannot close resources for req contexts:", e);
    }
  }

  // schedule config would be set at higher level
  @Override
  public boolean scheduled() {
    return false;
  }
}
