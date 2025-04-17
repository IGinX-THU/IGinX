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
package cn.edu.tsinghua.iginx.engine.distributedquery.coordinator;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.visitor.FragmentsVisitor;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;

public class NaiveEvaluator implements Evaluator {

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private final IMetaManager metaManager = DefaultMetaManager.getInstance();

  private static class NaiveEvaluatorHolder {
    private static final NaiveEvaluator INSTANCE = new NaiveEvaluator();
  }

  public static NaiveEvaluator getInstance() {
    return NaiveEvaluatorHolder.INSTANCE;
  }

  @Override
  public boolean needDistributedQuery(Operator root) {
    FragmentsVisitor visitor = new FragmentsVisitor();
    root.accept(visitor);
    int fragmentSize = visitor.getFragmentCount();
    int iginxSize = metaManager.getIginxList().size();
    return fragmentSize > config.getDistributedQueryTriggerThreshold()
        && fragmentSize >= iginxSize
        && iginxSize > 1;
  }
}
