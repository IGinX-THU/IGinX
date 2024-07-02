package cn.edu.tsinghua.iginx.session_v2;

import cn.edu.tsinghua.iginx.session_v2.exception.IginXException;
import cn.edu.tsinghua.iginx.thrift.TimePrecision;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface DeleteClient {

  void deleteMeasurement(final String measurement) throws IginXException;

  void deleteMeasurements(final Collection<String> measurements) throws IginXException;

  void deleteMeasurement(final Class<?> measurementType) throws IginXException;

  void deleteMeasurementData(final String measurement, long startKey, long endKey)
      throws IginXException;

  void deleteMeasurementData(
      final String measurement, long startKey, long endKey, TimePrecision timePrecisioin)
      throws IginXException;

  void deleteMeasurementsData(final Collection<String> measurements, long startKey, long endKey)
      throws IginXException;

  void deleteMeasurementsData(
      final Collection<String> measurements,
      long startKey,
      long endKey,
      TimePrecision timePrecisioin)
      throws IginXException;

  void deleteMeasurementData(final Class<?> measurementType, long startKey, long endKey)
      throws IginXException;

  void deleteMeasurementData(
      final Class<?> measurementType, long startKey, long endKey, TimePrecision timePrecisioin)
      throws IginXException;

  void deleteMeasurementData(
      final String measurement,
      long startKey,
      long endKey,
      List<Map<String, List<String>>> tagsList)
      throws IginXException;

  void deleteMeasurementData(
      final String measurement,
      long startKey,
      long endKey,
      List<Map<String, List<String>>> tagsList,
      TimePrecision timePrecisioin)
      throws IginXException;

  void deleteMeasurementsData(
      final Collection<String> measurements,
      long startKey,
      long endKey,
      List<Map<String, List<String>>> tagsList)
      throws IginXException;

  void deleteMeasurementsData(
      final Collection<String> measurements,
      long startKey,
      long endKey,
      List<Map<String, List<String>>> tagsList,
      TimePrecision timePrecisioin)
      throws IginXException;

  void deleteMeasurementData(
      final Class<?> measurementType,
      long startKey,
      long endKey,
      List<Map<String, List<String>>> tagsList)
      throws IginXException;

  void deleteMeasurementData(
      final Class<?> measurementType,
      long startKey,
      long endKey,
      List<Map<String, List<String>>> tagsList,
      TimePrecision timePrecisioin)
      throws IginXException;
}
