package cn.edu.tsinghua.iginx.session_v2.internal;

import cn.edu.tsinghua.iginx.session_v2.Arguments;
import cn.edu.tsinghua.iginx.session_v2.annotations.Field;
import cn.edu.tsinghua.iginx.session_v2.annotations.Measurement;
import cn.edu.tsinghua.iginx.session_v2.exception.IginXException;
import cn.edu.tsinghua.iginx.session_v2.write.Record;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MeasurementMapper {

  private static final Logger LOGGER = LoggerFactory.getLogger(MeasurementMapper.class);

  private static final ConcurrentMap<String, ConcurrentMap<String, java.lang.reflect.Field>>
      CLASS_FIELD_CACHE = new ConcurrentHashMap<>();

  <M> Record toRecord(final M measurement) throws IginXException {
    Arguments.checkNotNull(measurement, "measurement");

    Class<?> measurementType = measurement.getClass();
    cacheMeasurementClass(measurementType);

    if (measurementType.getAnnotation(Measurement.class) == null) {
      String message =
          String.format(
              "Measurement type '%s' does not have a @Measurement annotation.", measurementType);
      throw new IginXException(message);
    }

    Record.Builder recordBuilder = Record.builder();

    recordBuilder.measurement(getMeasurementName(measurementType));
    CLASS_FIELD_CACHE
        .get(measurementType.getName())
        .forEach(
            (name, field) -> {
              Field fieldAnnotation = field.getAnnotation(Field.class);

              Object value;
              try {
                field.setAccessible(true);
                value = field.get(measurement);
              } catch (IllegalAccessException e) {
                throw new IginXException(e);
              }

              if (value == null) {
                LOGGER.debug("Field {} of {} has null value", field.getName(), measurement);
                return;
              }

              Class<?> fieldType = field.getType();
              if (fieldAnnotation.timestamp()) {
                recordBuilder.key((Long) value);
              } else if (Boolean.class.isAssignableFrom(fieldType)
                  || boolean.class.isAssignableFrom(fieldType)) {
                recordBuilder.addBooleanField(name, (Boolean) value);
              } else if (Integer.class.isAssignableFrom(fieldType)
                  || int.class.isAssignableFrom(fieldType)) {
                recordBuilder.addIntField(name, (Integer) value);
              } else if (Long.class.isAssignableFrom(fieldType)
                  || long.class.isAssignableFrom(fieldType)) {
                recordBuilder.addLongField(name, (Long) value);
              } else if (Float.class.isAssignableFrom(fieldType)
                  || float.class.isAssignableFrom(fieldType)) {
                recordBuilder.addFloatField(name, (Float) value);
              } else if (Double.class.isAssignableFrom(fieldType)
                  || double.class.isAssignableFrom(fieldType)) {
                recordBuilder.addDoubleField(name, (Double) value);
              } else if (String.class.isAssignableFrom(fieldType)) {
                recordBuilder.addBinaryField(
                    name, ((String) value).getBytes(StandardCharsets.UTF_8));
              } else if (byte[].class.isAssignableFrom(fieldType)) {
                recordBuilder.addBinaryField(name, (byte[]) value);
              } else {
                recordBuilder.addBinaryField(
                    name, value.toString().getBytes(StandardCharsets.UTF_8));
              }
            });

    Record record = recordBuilder.build();

    LOGGER.debug("Mapped measurement: {} to record: {}", measurement, record);

    return record;
  }

  Collection<String> toMeasurements(final Class<?> measurementType) throws IginXException {
    cacheMeasurementClass(measurementType);

    if (measurementType.getAnnotation(Measurement.class) == null) {
      String message =
          String.format(
              "Measurement type '%s' does not have a @Measurement annotation.", measurementType);
      throw new IginXException(message);
    }

    Set<String> measurements = new HashSet<>();
    String measurement = getMeasurementName(measurementType);
    CLASS_FIELD_CACHE
        .get(measurementType.getName())
        .forEach(
            (name, field) -> {
              Field fieldAnnotation = field.getAnnotation(Field.class);

              if (!fieldAnnotation.timestamp()) {
                measurements.add(measurement + "." + name);
              }
            });
    return measurements;
  }

  private void cacheMeasurementClass(final Class<?> measurementType) {
    if (CLASS_FIELD_CACHE.containsKey(measurementType.getName())) {
      return;
    }
    ConcurrentMap<String, java.lang.reflect.Field> map = new ConcurrentHashMap<>();
    Class<?> currentMeasurementType = measurementType;
    while (currentMeasurementType != null) {
      for (java.lang.reflect.Field field : currentMeasurementType.getDeclaredFields()) {
        Field fieldAnnotation = field.getAnnotation(Field.class);
        if (fieldAnnotation != null) {
          String name = fieldAnnotation.name();
          if (name.isEmpty()) {
            name = field.getName();
          }
          map.put(name, field);
        }
      }

      currentMeasurementType = currentMeasurementType.getSuperclass();
    }

    CLASS_FIELD_CACHE.putIfAbsent(measurementType.getName(), map);
  }

  private String getMeasurementName(final Class<?> measurementType) {
    return measurementType.getAnnotation(Measurement.class).name();
  }
}
