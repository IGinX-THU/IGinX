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
package cn.edu.tsinghua.iginx.rest;

import cn.edu.tsinghua.iginx.IginxWorker;
import cn.edu.tsinghua.iginx.auth.SessionManager;
import cn.edu.tsinghua.iginx.security.SecurityConfig;
import cn.edu.tsinghua.iginx.thrift.*;
import cn.edu.tsinghua.iginx.utils.RpcUtils;
import java.security.Principal;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.core.SecurityContext;

/** One authenticated server session per HTTP request, explicitly carried across async dispatch. */
public final class RestRequest implements SecurityContext, AutoCloseable {
  private final long sessionId;
  private final String username;
  private final boolean tls;
  private final AtomicBoolean denied = new AtomicBoolean();

  public RestRequest(String username, String password, boolean tls) {
    OpenSessionResp response =
        IginxWorker.getInstance()
            .openSession(new OpenSessionReq().setUsername(username).setPassword(password));
    if (response.getStatus().getCode() != RpcUtils.SUCCESS.getCode()
        || response.getSessionId() == 0) {
      throw new NotAuthorizedException("Basic realm=\"IGinX\"");
    }
    this.sessionId = response.getSessionId();
    this.username = username;
    this.tls = tls;
  }

  public static RestRequest from(Object context) {
    if (!SecurityConfig.getInstance().isEnabled()) return null;
    if (!(context instanceof RestRequest))
      throw new NotAuthorizedException("Basic realm=\"IGinX\"");
    return (RestRequest) context;
  }

  public long sessionId() {
    if (denied.get() || !SessionManager.getInstance().isValidSession(sessionId)) reject();
    return sessionId;
  }

  public boolean isDenied() {
    return denied.get();
  }

  public void requireRpc(String... operations) {
    long id = sessionId();
    for (String operation : operations) {
      if (!cn.edu.tsinghua.iginx.auth.StatementPermissions.permitsRpc(id, operation)) reject();
    }
  }

  public void reject() {
    denied.set(true);
    close();
    throw new ForbiddenException("Access denied");
  }

  public void close() {
    SessionManager.getInstance().closeSession(sessionId);
  }

  public Principal getUserPrincipal() {
    return () -> username;
  }

  public boolean isUserInRole(String role) {
    return false;
  }

  public boolean isSecure() {
    return tls;
  }

  public String getAuthenticationScheme() {
    return BASIC_AUTH;
  }
}
