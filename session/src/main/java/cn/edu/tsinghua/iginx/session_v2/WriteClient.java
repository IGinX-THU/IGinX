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
