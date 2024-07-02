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
package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.compaction.CompactionManager;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.Result;
import cn.edu.tsinghua.iginx.utils.RpcUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompactStatement extends SystemStatement {
  private static final Logger LOGGER = LoggerFactory.getLogger(CompactStatement.class);

  public CompactStatement() {
    this.statementType = StatementType.COMPACT;
  }

  @Override
  public void execute(RequestContext ctx) {
    Result result = new Result(RpcUtils.SUCCESS);
    try {
      CompactionManager.getInstance().clearFragment();
      ctx.setResult(result);
    } catch (Exception e) {
      LOGGER.error("execute compact failed", e);
      ctx.setResult(new Result(RpcUtils.FAILURE));
    }
  }
}
