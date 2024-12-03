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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.register;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.register.system.ArithmeticExpr;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.register.system.Extract;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.register.system.Ratio;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SystemCalleeRegistry implements CalleeRegistry {

  private static final Logger LOGGER = LoggerFactory.getLogger(SystemCalleeRegistry.class);

  private static final SystemCalleeRegistry INSTANCE = new SystemCalleeRegistry();

  public static SystemCalleeRegistry getInstance() {
    return INSTANCE;
  }

  private SystemCalleeRegistry() {
    add(new ArithmeticExpr());
    add(new Extract());
    add(new Ratio());
  }

  public ConcurrentHashMap<String, Callee> calleeMap = new ConcurrentHashMap<>();

  @Override
  public boolean add(Callee callee) {
    AtomicBoolean success = new AtomicBoolean(false);
    calleeMap.computeIfAbsent(
        callee.getIdentifier(),
        k -> {
          success.set(true);
          return callee;
        });
    return success.get();
  }

  @Nullable
  @Override
  public Callee get(String identifier) {
    return calleeMap.get(identifier);
  }
}
