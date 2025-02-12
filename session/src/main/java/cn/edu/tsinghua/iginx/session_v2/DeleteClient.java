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
