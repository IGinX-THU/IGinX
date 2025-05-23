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
package cn.edu.tsinghua.iginx.resource;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.resource.exception.ResourceException;
import cn.edu.tsinghua.iginx.resource.system.DefaultSystemMetricsService;
import cn.edu.tsinghua.iginx.resource.system.SystemMetricsService;
import cn.edu.tsinghua.iginx.utils.RpcUtils;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResourceManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ResourceManager.class);

  private final SystemMetricsService systemMetrics;

  private final double heapMemoryThreshold;

  private final double systemMemoryThreshold;

  private final double systemCpuThreshold;

  private final RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);

  private ResourceManager() {
    Config config = ConfigDescriptor.getInstance().getConfig();
    switch (config.getSystemResourceMetrics()) {
      case "default":
        systemMetrics = new DefaultSystemMetricsService();
        break;
      default:
        LOGGER.info("use DefaultSystemMetrics as default");
        systemMetrics = new DefaultSystemMetricsService();
        break;
    }
    heapMemoryThreshold = config.getHeapMemoryThreshold();
    systemMemoryThreshold = config.getSystemMemoryThreshold();
    systemCpuThreshold = config.getSystemCpuThreshold();
  }

  public boolean reject(RequestContext ctx) {
    return heapMemoryOverwhelmed()
        || systemMetrics.getRecentCpuUsage() > systemCpuThreshold
        || systemMetrics.getRecentMemoryUsage() > systemMemoryThreshold;
  }

  public ResourceSet setup(RequestContext ctx) throws ResourceException {
    if (reject(ctx)) {
      throw new ResourceException(RpcUtils.SERVICE_UNAVAILABLE);
    }
    String name = String.format("request-%d", ctx.getId());
    BufferAllocator allocator = this.allocator.newChildAllocator(name, 0, Long.MAX_VALUE);
    ResourceSet resourceSet = new ResourceSet(allocator);
    ctx.setResourceSet(resourceSet);
    ctx.setAllocator(resourceSet.getAllocator());
    ctx.setConstantPool(resourceSet.getConstantPool());
    ctx.setTaskResultMap(resourceSet.getTaskResultMap());
    return resourceSet;
  }

  private boolean heapMemoryOverwhelmed() {
    return Runtime.getRuntime().totalMemory() * heapMemoryThreshold
        > Runtime.getRuntime().maxMemory();
  }

  public static ResourceManager getInstance() {
    return ResourceManagerHolder.INSTANCE;
  }

  private static class ResourceManagerHolder {

    private static final ResourceManager INSTANCE = new ResourceManager();
  }
}
