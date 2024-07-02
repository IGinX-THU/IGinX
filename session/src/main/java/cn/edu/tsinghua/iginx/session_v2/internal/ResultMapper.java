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
package cn.edu.tsinghua.iginx.session_v2.internal;

import cn.edu.tsinghua.iginx.session_v2.Arguments;
import cn.edu.tsinghua.iginx.session_v2.annotations.Field;
import cn.edu.tsinghua.iginx.session_v2.annotations.Measurement;
import cn.edu.tsinghua.iginx.session_v2.exception.IginXException;
import cn.edu.tsinghua.iginx.session_v2.query.IginXRecord;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResultMapper {
  @SuppressWarnings("unused")
  private static final Logger LOGGER = LoggerFactory.getLogger(ResultMapper.class);

  <T> T toPOJO(final IginXRecord record, final Class<T> clazz) {
    Arguments.checkNotNull(record, "record");
    Arguments.checkNotNull(clazz, "clazz");

    String measurement = clazz.getName();
    Measurement measurementAnno = clazz.getAnnotation(Measurement.class);
    if (measurementAnno != null) {
      measurement = measurementAnno.name();
    }

    try {
      T pojo = clazz.newInstance();

      Class<?> currentClazz = clazz;

      while (currentClazz != null) {

        java.lang.reflect.Field[] fields = currentClazz.getDeclaredFields();
        for (java.lang.reflect.Field field : fields) {
          Field anno = field.getAnnotation(Field.class);
          String fieldName = field.getName();

          if (anno != null && anno.timestamp()) {
            setFieldValue(pojo, field, record.getKey());
            continue;
          }

          if (anno != null && !anno.name().isEmpty()) {
            fieldName = anno.name();
          }

          if (!measurement.isEmpty()) {
            fieldName = measurement + "." + fieldName;
          }

          Map<String, Object> recordValues = record.getValues();
          if (recordValues.containsKey(fieldName)) {
            Object value = recordValues.get(fieldName);
            setFieldValue(pojo, field, value);
          }
        }

        currentClazz = currentClazz.getSuperclass();
      }

      return pojo;
    } catch (Exception e) {
      throw new IginXException(e);
    }
  }

  private void setFieldValue(
      final Object object, final java.lang.reflect.Field field, final Object value) {
    if (field == null || value == null) {
      return;
    }
    String msg =
        "Class '%s' field '%s' was defined with a different field type and caused a ClassCastException. "
            + "The correct type is '%s' (current field value: '%s').";

    try {
      if (!field.isAccessible()) {
        field.setAccessible(true);
      }
      Class<?> fieldType = field.getType();

      if (fieldType.equals(value.getClass())) {
        field.set(object, value);
        return;
      }
      if (double.class.isAssignableFrom(fieldType)) {
        field.setDouble(object, (double) value);
        return;
      }
      if (float.class.isAssignableFrom(fieldType)) {
        field.setFloat(object, (float) value);
        return;
      }
      if (long.class.isAssignableFrom(fieldType)) {
        field.setLong(object, (long) value);
        return;
      }
      if (int.class.isAssignableFrom(fieldType)) {
        field.setInt(object, (int) value);
        return;
      }
      if (boolean.class.isAssignableFrom(fieldType)) {
        field.setBoolean(object, Boolean.parseBoolean(String.valueOf(value)));
        return;
      }
      if (byte[].class.isAssignableFrom(fieldType)) {
        field.set(object, value);
      }
      if (String.class.isAssignableFrom(fieldType)) {
        field.set(object, new String((byte[]) value));
        return;
      }
      field.set(object, value);
    } catch (ClassCastException | IllegalAccessException e) {
      throw new IginXException(
          String.format(
              msg,
              object.getClass().getName(),
              field.getName(),
              value.getClass().getName(),
              value));
    }
  }
}
