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
package cn.edu.tsinghua.iginx.engine.physical.storage.descriptor;

import java.util.Map;

/**
 * Static capabilities of a driver, read before any instance of the driver is created, since the
 * constructor of a driver may connect to the service. Implementations must be lightweight: read
 * only the given options and resources on the classpath, never connect.
 *
 * <p>A driver declares its descriptor by {@link DriverDescriptor} on the class registered in {@code
 * databaseClassNames}. The descriptor is loaded by the class loader of the driver, so the driver
 * must not bundle its own copy of this interface.
 */
public interface StorageDriverDescriptor {

  StorageDriverDescriptor DEFAULT = options -> HostPolicy.RESOLVE_TO_IP;

  /** @param options extra params of the storage engine */
  HostPolicy hostPolicy(Map<String, String> options);
}
