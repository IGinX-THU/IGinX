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
package cn.edu.tsinghua.iginx.metadata.entity;

import cn.edu.tsinghua.iginx.transform.pojo.TriggerDescriptor;
import lombok.Data;

@Data
public class TransformJobMeta {
  private String name;
  private TriggerDescriptor trigger;
  private String ip;
  private int port;

  public TransformJobMeta(String name, TriggerDescriptor trigger, String ip, int port) {
    this.name = name;
    this.trigger = trigger;
    this.ip = ip;
    this.port = port;
  }

  public TransformJobMeta copy() {
    return new TransformJobMeta(name, trigger.copy(), ip, port);
  }
}
