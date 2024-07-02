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
