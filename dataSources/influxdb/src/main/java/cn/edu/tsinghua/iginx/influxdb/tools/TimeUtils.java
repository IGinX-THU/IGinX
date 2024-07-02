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
package cn.edu.tsinghua.iginx.influxdb.tools;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class TimeUtils {

  private static final DateTimeFormatter FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");

  public static long instantToNs(Instant time) {
    return time.getEpochSecond() * 1_000_000_000L + time.getNano();
  }

  public static String nanoTimeToStr(long nanoTime) {
    long remainder = nanoTime % 1_000_000;
    long timeInMs = nanoTime / 1_000_000;
    return ZonedDateTime.ofInstant(Instant.ofEpochMilli(timeInMs), ZoneId.of("UTC"))
            .format(FORMATTER)
        + String.format("%06d", remainder)
        + 'Z';
  }
}
