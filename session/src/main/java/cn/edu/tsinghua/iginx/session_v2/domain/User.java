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
package cn.edu.tsinghua.iginx.session_v2.domain;

import cn.edu.tsinghua.iginx.thrift.AuthType;
import cn.edu.tsinghua.iginx.thrift.UserType;
import java.util.Set;

public final class User {

  private final String username;

  private final String password;

  private final UserType userType;

  private final Set<AuthType> auths;

  public User(String username, UserType userType, Set<AuthType> auths) {
    this(username, null, userType, auths);
  }

  public User(String username, String password, UserType userType, Set<AuthType> auths) {
    this.username = username;
    this.password = password;
    this.userType = userType;
    this.auths = auths;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public UserType getUserType() {
    return userType;
  }

  public Set<AuthType> getAuths() {
    return auths;
  }
}
