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

import cn.edu.tsinghua.iginx.security.SecurityConfig;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import javax.annotation.Priority;
import javax.ws.rs.Priorities;
import javax.ws.rs.container.*;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;

@Provider
@PreMatching
@Priority(Priorities.AUTHENTICATION)
public final class RestAuthenticationFilter
    implements ContainerRequestFilter, ContainerResponseFilter {
  private static final String IDENTITY = RestRequest.class.getName();

  public void filter(ContainerRequestContext request) {
    if (!SecurityConfig.getInstance().isEnabled()) return;
    try {
      String header = request.getHeaderString("Authorization");
      if (header == null || !header.regionMatches(true, 0, "Basic ", 0, 6))
        throw new IllegalArgumentException();
      String pair =
          new String(
              Base64.getDecoder().decode(header.substring(6).trim()), StandardCharsets.UTF_8);
      int colon = pair.indexOf(':');
      if (colon <= 0) throw new IllegalArgumentException();
      RestRequest identity =
          new RestRequest(
              pair.substring(0, colon),
              pair.substring(colon + 1),
              request.getSecurityContext().isSecure());
      request.setProperty(IDENTITY, identity);
      request.setSecurityContext(identity);
    } catch (IllegalArgumentException | javax.ws.rs.NotAuthorizedException e) {
      request.abortWith(
          Response.status(401).header("WWW-Authenticate", "Basic realm=\"IGinX\"").build());
    }
  }

  public void filter(ContainerRequestContext request, ContainerResponseContext response) {
    RestRequest identity = (RestRequest) request.getProperty(IDENTITY);
    if (identity != null) {
      if (identity.isDenied()) {
        response.setStatus(403);
        response.setEntity("Access denied");
      }
      identity.close();
    }
  }
}
