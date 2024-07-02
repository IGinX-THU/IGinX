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
package cn.edu.tsinghua.iginx.jdbc;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Utils {
  private static final Logger LOGGER = Logger.getLogger(Utils.class.getName());

  private static final Pattern URL_PATTERN = Pattern.compile("([^:]+):([0-9]{1,5})/?");
  private static final DateTimeFormatter milliSecFormatter =
      new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd HH:mm:ss.SSS").toFormatter();
  private static final DateTimeFormatter microSecFormatter =
      new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd HH:mm:ss.SSSSSS").toFormatter();
  private static final DateTimeFormatter nanoSecFormatter =
      new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS").toFormatter();

  // The only support format of the URL is:
  // jdbc:iginx://localhost:6667/
  public static IginXConnectionParams parseUrl(String url, Properties info)
      throws IginxUrlException {
    IginXConnectionParams params = new IginXConnectionParams();

    url = url.trim();
    if (url.equalsIgnoreCase(Config.IGINX_URL_PREFIX)) {
      return params;
    }

    boolean isUrlLegal = false;
    Matcher matcher = null;
    if (url.startsWith(Config.IGINX_URL_PREFIX)) {
      String subURL = url.substring(Config.IGINX_URL_PREFIX.length());
      matcher = URL_PATTERN.matcher(subURL);
      if (matcher.matches()) {
        isUrlLegal = true;
      }
    }

    if (!isUrlLegal) {
      throw new IginxUrlException(
          "Error url format, " + "url should be jdbc:iginx://ip:port/ or jdbc:iginx://ip:port");
    }

    params.setHost(matcher.group(1));
    params.setPort(Integer.parseInt(matcher.group(2)));

    if (info.containsKey(Config.USER)) {
      params.setUsername(info.getProperty(Config.USER));
    }
    if (info.containsKey(Config.PASSWORD)) {
      params.setPassword(info.getProperty(Config.PASSWORD));
    }

    return params;
  }

  public static Time parseTime(String timestampStr) throws DateTimeParseException {
    LocalDateTime dateTime = parseLocalDateTime(timestampStr);
    return dateTime != null ? Time.valueOf(dateTime.toLocalTime()) : null;
  }

  public static Date parseDate(String timestampStr) {
    LocalDateTime dateTime = parseLocalDateTime(timestampStr);
    return dateTime != null ? Date.valueOf(String.valueOf(dateTime)) : null;
  }

  public static Timestamp parseTimestamp(String timeStampStr) {
    LocalDateTime dateTime = parseLocalDateTime(timeStampStr);
    return dateTime != null ? Timestamp.valueOf(dateTime) : null;
  }

  private static LocalDateTime parseLocalDateTime(String timeStampStr) {
    try {
      return parseMilliSecTimestamp(timeStampStr);
    } catch (DateTimeParseException e) {
      try {
        return parseMicroSecTimestamp(timeStampStr);
      } catch (DateTimeParseException ee) {
        try {
          return parseNanoSecTimestamp(timeStampStr);
        } catch (DateTimeParseException eee) {
          LOGGER.log(Level.SEVERE, "unexpected error: ", e);
        }
      }
    }
    return null;
  }

  private static LocalDateTime parseMilliSecTimestamp(String timeStampStr)
      throws DateTimeParseException {
    return LocalDateTime.parse(timeStampStr, milliSecFormatter);
  }

  private static LocalDateTime parseMicroSecTimestamp(String timeStampStr)
      throws DateTimeParseException {
    return LocalDateTime.parse(timeStampStr, microSecFormatter);
  }

  private static LocalDateTime parseNanoSecTimestamp(String timeStampStr)
      throws DateTimeParseException {
    return LocalDateTime.parse(timeStampStr, nanoSecFormatter);
  }

  public static String formatTimestamp(Timestamp timestamp) {
    int nanos = timestamp.getNanos();
    if (nanos % 1000000L != 0) return timestamp.toLocalDateTime().format(microSecFormatter);
    return timestamp.toLocalDateTime().format(milliSecFormatter);
  }

  public static byte[] IntToByteArray(int value) {
    return new byte[] {
      (byte) (value >> 24), (byte) (value >> 16), (byte) (value >> 8), (byte) value
    };
  }

  public static byte[] ShortToByteArray(short value) {
    return new byte[] {(byte) (value >> 8), (byte) value};
  }

  public static byte[] LongToByteArray(long value) {
    byte[] result = new byte[8];

    for (int i = 7; i >= 0; --i) {
      result[i] = (byte) ((int) (value & 255L));
      value >>= 8;
    }

    return result;
  }
}
