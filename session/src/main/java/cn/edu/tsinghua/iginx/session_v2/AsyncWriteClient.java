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
package cn.edu.tsinghua.iginx.session_v2;

import cn.edu.tsinghua.iginx.session_v2.write.Point;
import cn.edu.tsinghua.iginx.session_v2.write.Record;
import cn.edu.tsinghua.iginx.session_v2.write.Table;
import cn.edu.tsinghua.iginx.thrift.TimePrecision;
import java.util.List;

public interface AsyncWriteClient extends AutoCloseable {

  void writePoint(final Point point);

  void writePoint(final Point pointm, final TimePrecision timePrecision);

  void writePoints(final List<Point> points);

  void writePoints(final List<Point> points, final TimePrecision timePrecision);

  void writeRecord(final Record record);

  void writeRecord(final Record record, final TimePrecision timePrecision);

  void writeRecords(final List<Record> records);

  void writeRecords(final List<Record> records, final TimePrecision timePrecision);

  <M> void writeMeasurement(final M measurement);

  <M> void writeMeasurement(final M measurement, final TimePrecision timePrecision);

  <M> void writeMeasurements(final List<M> measurements);

  <M> void writeMeasurements(final List<M> measurements, final TimePrecision timePrecision);

  void writeTable(final Table table);

  void writeTable(final Table table, final TimePrecision timePrecision);

  @Override
  void close() throws Exception;
}
