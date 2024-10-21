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
package cn.edu.tsinghua.iginx.policy;

import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PolicyManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(PolicyManager.class);

  private static final PolicyManager instance = new PolicyManager();

  private final Map<String, IPolicy> policies;

  private PolicyManager() {
    this.policies = new HashMap<>();
  }

  public static PolicyManager getInstance() {
    return instance;
  }

  public IPolicy getPolicy(String policyClassName) {
    IPolicy policy;
    synchronized (policies) {
      policy = policies.get(policyClassName);
      if (policy == null) {
        try {
          Class<? extends IPolicy> clazz =
              this.getClass().getClassLoader().loadClass(policyClassName).asSubclass(IPolicy.class);
          policy = clazz.getConstructor().newInstance();
          policy.init(DefaultMetaManager.getInstance());
          policies.put(policyClassName, policy);
        } catch (ClassNotFoundException
            | InstantiationException
            | IllegalAccessException
            | NoSuchMethodException
            | InvocationTargetException e) {
          LOGGER.error("Failed to load policy: {}", policyClassName, e);
        }
      }
    }
    return policy;
  }
}
