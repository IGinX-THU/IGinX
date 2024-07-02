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

import cn.edu.tsinghua.iginx.IginxWorker;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.Result;
import cn.edu.tsinghua.iginx.engine.shared.exception.StatementExecutionException;
import cn.edu.tsinghua.iginx.thrift.RegisterTaskReq;
import cn.edu.tsinghua.iginx.thrift.Status;
import cn.edu.tsinghua.iginx.thrift.UDFClassPair;
import cn.edu.tsinghua.iginx.thrift.UDFType;
import cn.edu.tsinghua.iginx.utils.RpcUtils;
import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegisterTaskStatement extends SystemStatement {
  @SuppressWarnings("unused")
  private static final Logger LOGGER = LoggerFactory.getLogger(RegisterTaskStatement.class);

  private final String filePath;

  private final List<UDFClassPair> pairs;

  private final List<UDFType> types;

  private final IginxWorker worker = IginxWorker.getInstance();

  public RegisterTaskStatement(String filePath, List<UDFClassPair> pairs, List<UDFType> types) {
    this.statementType = StatementType.REGISTER_TASK;
    this.pairs = pairs;
    this.filePath = filePath;
    this.types = types;
  }

  @Override
  public void execute(RequestContext ctx) throws StatementExecutionException {
    File file = new File(filePath);
    // in two conditions we need extra information: remote && no buffer; local && relative filepath
    if ((ctx.getUDFModuleByteBuffer() == null && ctx.isRemoteUDF())
        || (!ctx.isRemoteUDF() && !file.isAbsolute())) {
      ctx.setResult(new Result(RpcUtils.SUCCESS));
      ctx.getResult().setUDFModulePath(filePath);
      return;
    }

    // validate meta data first
    Status status = metadataValidate();
    if (status.code != RpcUtils.SUCCESS.code) {
      ctx.setResult(new Result(status));
      return;
    }

    RegisterTaskReq req =
        new RegisterTaskReq(
            ctx.getSessionId(),
            filePath,
            pairs,
            types,
            ctx.getUDFModuleByteBuffer(),
            ctx.isRemoteUDF());
    status = worker.registerTask(req);
    ctx.setResult(new Result(status));
    ctx.getResult().setUDFModulePath(filePath);
  }

  private Status metadataValidate() {
    String errorMsg;
    // fail if type's count doesn't match names.
    if (pairs.size() != types.size() && types.size() > 1) {
      errorMsg =
          String.format(
              "Fail to register %d UDFs with %d types, the number should be same or use only one type.",
              pairs.size(), types.size());
      LOGGER.error(errorMsg);
      return RpcUtils.FAILURE.setMessage(errorMsg);
    }

    // fail if trying to register UDFs with same class name or name.
    Set<String> tempName = new HashSet<>();
    Set<String> tempClass = new HashSet<>();
    for (UDFClassPair p : pairs) {
      if (!tempName.add(p.name)) {
        errorMsg = String.format("Cannot register multiple UDFs with same name: %s", p.name);
        LOGGER.error(errorMsg);
        return RpcUtils.FAILURE.setMessage(errorMsg);
      }
      if (!tempClass.add(p.classPath)) {
        errorMsg = String.format("Cannot register multiple UDFs with same class: %s", p.classPath);
        LOGGER.error(errorMsg);
        return RpcUtils.FAILURE.setMessage(errorMsg);
      }
    }
    return RpcUtils.SUCCESS;
  }
}
