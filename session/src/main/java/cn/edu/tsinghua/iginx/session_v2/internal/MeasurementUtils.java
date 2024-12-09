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
package cn.edu.tsinghua.iginx.session_v2.internal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

class MeasurementUtils {

  static List<String> mergeAndSortMeasurements(List<String> measurements) {
    if (measurements.stream().anyMatch(x -> x.equals("*"))) {
      List<String> tempPaths = new ArrayList<>();
      tempPaths.add("*");
      return tempPaths;
    }
    List<String> prefixes =
        measurements.stream()
            .filter(x -> x.contains("*"))
            .map(x -> x.substring(0, x.indexOf("*")))
            .collect(Collectors.toList());
    if (prefixes.isEmpty()) {
      Collections.sort(measurements);
      return measurements;
    }
    List<String> mergedMeasurements = new ArrayList<>();
    for (String measurement : measurements) {
      if (!measurement.contains("*")) {
        boolean skip = false;
        for (String prefix : prefixes) {
          if (measurement.startsWith(prefix)) {
            skip = true;
            break;
          }
        }
        if (skip) {
          continue;
        }
      }
      mergedMeasurements.add(measurement);
    }
    mergedMeasurements.sort(String::compareTo);
    return mergedMeasurements;
  }
}
