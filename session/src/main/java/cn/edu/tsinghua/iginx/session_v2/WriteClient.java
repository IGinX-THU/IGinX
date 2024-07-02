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
package cn.edu.tsinghua.iginx.session_v2;

import cn.edu.tsinghua.iginx.session_v2.exception.IginXException;
import cn.edu.tsinghua.iginx.session_v2.write.Point;
import cn.edu.tsinghua.iginx.session_v2.write.Record;
import cn.edu.tsinghua.iginx.session_v2.write.Table;
import cn.edu.tsinghua.iginx.thrift.TimePrecision;
import java.util.List;

public interface WriteClient {

  void writePoint(final Point point) throws IginXException;

  void writePoint(final Point point, final TimePrecision timePrecision) throws IginXException;

  void writePoints(final List<Point> points) throws IginXException;

  void writePoints(final List<Point> points, final TimePrecision timePrecision)
      throws IginXException;

  void writeRecord(final Record record) throws IginXException;

  void writeRecord(final Record record, final TimePrecision timePrecision) throws IginXException;

  void writeRecords(final List<Record> records) throws IginXException;

  void writeRecords(final List<Record> records, final TimePrecision timePrecision)
      throws IginXException;

  <M> void writeMeasurement(final M measurement) throws IginXException;

  <M> void writeMeasurement(final M measurement, final TimePrecision timePrecision)
      throws IginXException;

  <M> void writeMeasurements(final List<M> measurements) throws IginXException;

  <M> void writeMeasurements(final List<M> measurements, final TimePrecision timePrecision)
      throws IginXException;

  void writeTable(final Table table) throws IginXException;

  void writeTable(final Table table, final TimePrecision timePrecision) throws IginXException;
}
