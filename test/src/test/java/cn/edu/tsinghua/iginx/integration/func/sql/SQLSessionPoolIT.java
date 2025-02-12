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
package cn.edu.tsinghua.iginx.integration.func.sql;

import static cn.edu.tsinghua.iginx.integration.controller.Controller.SUPPORT_KEY;

import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.integration.tool.ConfLoader;
import java.io.IOException;
import org.junit.*;

public class SQLSessionPoolIT extends SQLSessionIT {
  public SQLSessionPoolIT() throws IOException {
    super();
    ConfLoader conf = new ConfLoader(Controller.CONFIG_FILE);
    if (!SUPPORT_KEY.get(conf.getStorageType()) && this.isScaling) {
      needCompareResult = false;
      executor.setNeedCompareResult(needCompareResult);
    }
    isForSessionPool = true;
    isForSession = false;
    MaxMultiThreadTaskNum = 10;
  }
}
