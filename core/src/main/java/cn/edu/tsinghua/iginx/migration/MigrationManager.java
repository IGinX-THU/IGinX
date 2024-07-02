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
package cn.edu.tsinghua.iginx.migration;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MigrationManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(MigrationManager.class);

  private static final MigrationManager instance = new MigrationManager();

  private final Map<String, MigrationPolicy> policies;

  private MigrationManager() {
    this.policies = new HashMap<>();
  }

  public static MigrationManager getInstance() {
    return instance;
  }

  public MigrationPolicy getMigration() {
    String policyClassName =
        ConfigDescriptor.getInstance().getConfig().getMigrationPolicyClassName();
    MigrationPolicy policy;
    synchronized (policies) {
      policy = policies.get(policyClassName);
      if (policy == null) {
        try {
          Class<? extends MigrationPolicy> clazz =
              this.getClass()
                  .getClassLoader()
                  .loadClass(policyClassName)
                  .asSubclass(MigrationPolicy.class);
          policy = clazz.getConstructor().newInstance();
          policies.put(policyClassName, policy);
        } catch (Exception e) {
          LOGGER.error("Failed to load policy: {}", policyClassName, e);
        }
      }
    }
    return policy;
  }
}
