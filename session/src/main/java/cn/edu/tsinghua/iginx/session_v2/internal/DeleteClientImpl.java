package cn.edu.tsinghua.iginx.session_v2.internal;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.session_v2.Arguments;
import cn.edu.tsinghua.iginx.session_v2.DeleteClient;
import cn.edu.tsinghua.iginx.session_v2.exception.IginXException;
import cn.edu.tsinghua.iginx.thrift.DeleteColumnsReq;
import cn.edu.tsinghua.iginx.thrift.DeleteDataInColumnsReq;
import cn.edu.tsinghua.iginx.thrift.Status;
import cn.edu.tsinghua.iginx.thrift.TimePrecision;
import cn.edu.tsinghua.iginx.utils.StatusUtils;
import java.util.*;
import org.apache.thrift.TException;

public class DeleteClientImpl extends AbstractFunctionClient implements DeleteClient {

  private final MeasurementMapper measurementMapper;

  public DeleteClientImpl(IginXClientImpl iginXClient, MeasurementMapper measurementMapper) {
    super(iginXClient);
    this.measurementMapper = measurementMapper;
  }

  @Override
  public void deleteMeasurement(String measurement) throws IginXException {
    Arguments.checkNotNull(measurement, "measurement");
    deleteMeasurements(Collections.singletonList(measurement));
  }

  @Override
  public void deleteMeasurements(Collection<String> measurements) throws IginXException {
    Arguments.checkNotNull(measurements, "measurements");
    measurements.forEach(measurement -> Arguments.checkNotNull(measurement, "measurement"));

    DeleteColumnsReq req =
        new DeleteColumnsReq(
            sessionId, MeasurementUtils.mergeAndSortMeasurements(new ArrayList<>(measurements)));

    synchronized (iginXClient) {
      iginXClient.checkIsClosed();
      try {
        Status status = client.deleteColumns(req);
        StatusUtils.verifySuccess(status);
      } catch (TException | SessionException e) {
        throw new IginXException("delete measurements failure: ", e);
      }
    }
  }

  @Override
  public void deleteMeasurement(Class<?> measurementType) throws IginXException {
    Arguments.checkNotNull(measurementType, "measurementType");
    Collection<String> measurements = measurementMapper.toMeasurements(measurementType);
    deleteMeasurements(measurements);
  }

  @Override
  public void deleteMeasurementData(String measurement, long startKey, long endKey)
      throws IginXException {
    deleteMeasurementData(measurement, startKey, endKey, null, null);
  }

  @Override
  public void deleteMeasurementData(
      String measurement, long startKey, long endKey, TimePrecision timePrecision)
      throws IginXException {
    deleteMeasurementData(measurement, startKey, endKey, null, timePrecision);
  }

  @Override
  public void deleteMeasurementsData(Collection<String> measurements, long startKey, long endKey)
      throws IginXException {
    deleteMeasurementsData(measurements, startKey, endKey, null, null);
  }

  @Override
  public void deleteMeasurementsData(
      Collection<String> measurements, long startKey, long endKey, TimePrecision timePrecision)
      throws IginXException {
    deleteMeasurementsData(measurements, startKey, endKey, null, timePrecision);
  }

  @Override
  public void deleteMeasurementData(Class<?> measurementType, long startKey, long endKey)
      throws IginXException {
    deleteMeasurementData(measurementType, startKey, endKey, null, null);
  }

  @Override
  public void deleteMeasurementData(
      Class<?> measurementType, long startKey, long endKey, TimePrecision timePrecision)
      throws IginXException {
    deleteMeasurementData(measurementType, startKey, endKey, null, timePrecision);
  }

  @Override
  public void deleteMeasurementData(
      String measurement, long startKey, long endKey, List<Map<String, List<String>>> tagsList)
      throws IginXException {
    Arguments.checkNotNull(measurement, "measurement");
    deleteMeasurementsData(
        Collections.singletonList(measurement), startKey, endKey, tagsList, null);
  }

  @Override
  public void deleteMeasurementData(
      String measurement,
      long startKey,
      long endKey,
      List<Map<String, List<String>>> tagsList,
      TimePrecision timePrecision)
      throws IginXException {
    Arguments.checkNotNull(measurement, "measurement");
    deleteMeasurementsData(
        Collections.singletonList(measurement), startKey, endKey, tagsList, timePrecision);
  }

  @Override
  public void deleteMeasurementsData(
      Collection<String> measurements,
      long startKey,
      long endKey,
      List<Map<String, List<String>>> tagsList)
      throws IginXException {
    deleteMeasurementsData(measurements, startKey, endKey, tagsList, null);
  }

  @Override
  public void deleteMeasurementsData(
      Collection<String> measurements,
      long startKey,
      long endKey,
      List<Map<String, List<String>>> tagsList,
      TimePrecision timePrecision)
      throws IginXException {
    Arguments.checkNotNull(measurements, "measurements");
    measurements.forEach(measurement -> Arguments.checkNotNull(measurement, "measurement"));

    DeleteDataInColumnsReq req =
        new DeleteDataInColumnsReq(
            sessionId,
            MeasurementUtils.mergeAndSortMeasurements(new ArrayList<>(measurements)),
            startKey,
            endKey);

    if (tagsList != null && !tagsList.isEmpty()) {
      req.setTagsList(tagsList);
    }
    req.setTimePrecision(timePrecision);

    synchronized (iginXClient) {
      iginXClient.checkIsClosed();
      try {
        Status status = client.deleteDataInColumns(req);
        StatusUtils.verifySuccess(status);
      } catch (TException | SessionException e) {
        throw new IginXException("delete measurements data failure: ", e);
      }
    }
  }

  @Override
  public void deleteMeasurementData(
      Class<?> measurementType,
      long startKey,
      long endKey,
      List<Map<String, List<String>>> tagsList)
      throws IginXException {
    Arguments.checkNotNull(measurementType, "measurementType");
    Collection<String> measurements = measurementMapper.toMeasurements(measurementType);
    deleteMeasurementsData(measurements, startKey, endKey, tagsList);
  }

  @Override
  public void deleteMeasurementData(
      Class<?> measurementType,
      long startKey,
      long endKey,
      List<Map<String, List<String>>> tagsList,
      TimePrecision timePrecision)
      throws IginXException {
    Arguments.checkNotNull(measurementType, "measurementType");
    Collection<String> measurements = measurementMapper.toMeasurements(measurementType);
    deleteMeasurementsData(measurements, startKey, endKey, tagsList, timePrecision);
  }
}
