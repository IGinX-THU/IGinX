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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.session_v2.internal;

import cn.edu.tsinghua.iginx.session_v2.AsyncWriteClient;
import cn.edu.tsinghua.iginx.session_v2.WriteClient;
import cn.edu.tsinghua.iginx.session_v2.write.Point;
import cn.edu.tsinghua.iginx.session_v2.write.Record;
import cn.edu.tsinghua.iginx.session_v2.write.Table;
import cn.edu.tsinghua.iginx.thrift.TimePrecision;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncWriteClientImpl extends AbstractFunctionClient implements AsyncWriteClient {
  @SuppressWarnings("unused")
  private static final Logger LOGGER = LoggerFactory.getLogger(AsyncWriteClientImpl.class);

  private final WriteClient syncWriteClient; // 内部执行还是使用同步客户端来执行的

  private final Collection<AutoCloseable> autoCloseables;

  private final ExecutorService asyncWriteService;

  private final MeasurementMapper measurementMapper;

  public AsyncWriteClientImpl(
      IginXClientImpl iginXClient,
      MeasurementMapper measurementMapper,
      Collection<AutoCloseable> autoCloseables) {
    super(iginXClient);

    this.autoCloseables = autoCloseables;
    this.measurementMapper = measurementMapper;

    this.syncWriteClient = new WriteClientImpl(iginXClient, measurementMapper);

    this.asyncWriteService = Executors.newSingleThreadExecutor();
    autoCloseables.add(this);
  }

  @Override
  public void writePoint(Point point) {
    writePoints(Collections.singletonList(point), null);
  }

  @Override
  public void writePoint(Point point, TimePrecision timePrecision) {
    writePoints(Collections.singletonList(point), timePrecision);
  }

  @Override
  public void writePoints(List<Point> points) {
    asyncWriteService.execute(newAsyncWritePointsTask(points, null));
  }

  @Override
  public void writePoints(List<Point> points, TimePrecision timePrecision) {
    asyncWriteService.execute(newAsyncWritePointsTask(points, timePrecision));
  }

  @Override
  public void writeRecord(Record record) {
    writeRecords(Collections.singletonList(record), null);
  }

  @Override
  public void writeRecord(Record record, TimePrecision timePrecision) {
    writeRecords(Collections.singletonList(record), timePrecision);
  }

  @Override
  public void writeRecords(List<Record> records) {
    asyncWriteService.execute(newAsyncWriteRecordsTask(records));
  }

  @Override
  public void writeRecords(List<Record> records, TimePrecision timePrecision) {
    asyncWriteService.execute(newAsyncWriteRecordsTask(records, timePrecision));
  }

  @Override
  public <M> void writeMeasurement(M measurement) {
    writeMeasurements(Collections.singletonList(measurement), null);
  }

  @Override
  public <M> void writeMeasurement(M measurement, TimePrecision timePrecision) {
    writeMeasurements(Collections.singletonList(measurement), timePrecision);
  }

  @Override
  public <M> void writeMeasurements(List<M> measurements) {
    asyncWriteService.execute(
        newAsyncWriteRecordsTask(
            measurements.stream().map(measurementMapper::toRecord).collect(Collectors.toList())));
  }

  @Override
  public <M> void writeMeasurements(List<M> measurements, TimePrecision timePrecision) {
    asyncWriteService.execute(
        newAsyncWriteRecordsTask(
            measurements.stream().map(measurementMapper::toRecord).collect(Collectors.toList()),
            timePrecision));
  }

  @Override
  public void writeTable(Table table) {
    asyncWriteService.execute(newAsyncWriteTableTask(table));
  }

  @Override
  public void writeTable(Table table, TimePrecision timePrecision) {
    asyncWriteService.execute(newAsyncWriteTableTask(table, timePrecision));
  }

  @Override
  public void close() throws Exception {
    autoCloseables.remove(this);
    asyncWriteService.shutdown();
    if (!asyncWriteService.awaitTermination(10, TimeUnit.SECONDS)) {
      asyncWriteService.shutdownNow();
    }
  }

  private AsyncWriteTask newAsyncWritePointsTask(List<Point> points) {
    return new AsyncWriteTask(points, null, null);
  }

  private AsyncWriteTask newAsyncWritePointsTask(List<Point> points, TimePrecision timePrecison) {
    return new AsyncWriteTask(points, null, null, timePrecison);
  }

  private AsyncWriteTask newAsyncWriteRecordsTask(List<Record> records) {
    return new AsyncWriteTask(null, records, null);
  }

  private AsyncWriteTask newAsyncWriteRecordsTask(
      List<Record> records, TimePrecision timePrecison) {
    return new AsyncWriteTask(null, records, null, timePrecison);
  }

  private AsyncWriteTask newAsyncWriteTableTask(Table table) {
    return new AsyncWriteTask(null, null, table);
  }

  private AsyncWriteTask newAsyncWriteTableTask(Table table, TimePrecision timePrecison) {
    return new AsyncWriteTask(null, null, table, timePrecison);
  }

  private class AsyncWriteTask implements Runnable {

    final List<Point> points;

    final List<Record> records;

    final Table table;

    final TimePrecision timePrecision;

    AsyncWriteTask(List<Point> points, List<Record> records, Table table) {
      this.points = points;
      this.records = records;
      this.table = table;
      this.timePrecision = null;
    }

    AsyncWriteTask(
        List<Point> points, List<Record> records, Table table, TimePrecision timePrecision) {
      this.points = points;
      this.records = records;
      this.table = table;
      this.timePrecision = timePrecision;
    }

    @Override
    public void run() {
      if (points != null) {
        AsyncWriteClientImpl.this.syncWriteClient.writePoints(points, timePrecision);
      } else if (records != null) {
        AsyncWriteClientImpl.this.syncWriteClient.writeRecords(records, timePrecision);
      } else if (table != null) {
        AsyncWriteClientImpl.this.syncWriteClient.writeTable(table, timePrecision);
      }
    }
  }
}
