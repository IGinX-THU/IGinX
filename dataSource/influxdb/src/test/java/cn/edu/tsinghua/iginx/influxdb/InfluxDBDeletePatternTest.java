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
package cn.edu.tsinghua.iginx.influxdb;

import static cn.edu.tsinghua.iginx.influxdb.InfluxDBStorage.getDeletedMeasurements;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import cn.edu.tsinghua.iginx.engine.shared.operator.tag.BaseTagFilter;
import cn.edu.tsinghua.iginx.influxdb.exception.InfluxDBTaskExecuteFailureException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class InfluxDBDeletePatternTest {

  @Test
  public void deleteAll() throws InfluxDBTaskExecuteFailureException {
    assertNull(getDeletedMeasurements(Collections.emptyList(), null));
    assertNull(getDeletedMeasurements(Collections.singletonList("*"), null));
    assertNull(getDeletedMeasurements(Collections.singletonList("*.*"), null));
    assertNull(getDeletedMeasurements(Arrays.asList("a.*", "*"), null));
    // "*" covers every other pattern, wherever it appears
    assertNull(getDeletedMeasurements(Arrays.asList("*.*", "b.c"), null));
    assertNull(getDeletedMeasurements(Arrays.asList("b.c", "*"), null));
  }

  @Test
  public void deleteMeasurements() throws InfluxDBTaskExecuteFailureException {
    assertEquals(
        Collections.singletonList("a"),
        getDeletedMeasurements(Collections.singletonList("a.*"), null));
    assertEquals(
        Arrays.asList("a", "b"), getDeletedMeasurements(Arrays.asList("a.*", "b.*", "a.*"), null));
  }

  @Test
  public void rejectPartialMeasurements() {
    List<String> rejected =
        Arrays.asList("a.b", "a.b.*", "*.b", "a*.*", "a", "unit0000000001*", ".*", "a.*b");
    for (String pattern : rejected) {
      assertThrows(
          pattern,
          InfluxDBTaskExecuteFailureException.class,
          () -> getDeletedMeasurements(Collections.singletonList(pattern), null));
    }
    // one unsupported pattern rejects the whole request, so nothing is deleted
    assertThrows(
        InfluxDBTaskExecuteFailureException.class,
        () -> getDeletedMeasurements(Arrays.asList("a.*", "b.c"), null));
  }

  @Test
  public void rejectTagFilter() {
    assertThrows(
        InfluxDBTaskExecuteFailureException.class,
        () ->
            getDeletedMeasurements(Collections.singletonList("a.*"), new BaseTagFilter("k", "v")));
    assertThrows(
        InfluxDBTaskExecuteFailureException.class,
        () -> getDeletedMeasurements(Collections.singletonList("*"), new BaseTagFilter("k", "v")));
  }
}
