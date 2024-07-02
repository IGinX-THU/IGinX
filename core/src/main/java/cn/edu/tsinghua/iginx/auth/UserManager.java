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
package cn.edu.tsinghua.iginx.auth;

import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.UserMeta;
import cn.edu.tsinghua.iginx.thrift.AuthType;
import cn.edu.tsinghua.iginx.thrift.UserType;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UserManager {
  @SuppressWarnings("unused")
  private static final Logger LOGGER = LoggerFactory.getLogger(UserManager.class);

  private static UserManager instance;

  private final IMetaManager metaManager;

  private UserManager(IMetaManager metaManager) {
    this.metaManager = metaManager;
  }

  public static UserManager getInstance() {
    if (instance == null) {
      synchronized (UserManager.class) {
        if (instance == null) {
          instance = new UserManager(DefaultMetaManager.getInstance());
        }
      }
    }
    return instance;
  }

  public boolean hasUser(String username) {
    UserMeta user = metaManager.getUser(username);
    return user != null;
  }

  public boolean checkUser(String username, String password) {
    UserMeta user = metaManager.getUser(username);
    return user != null && user.getPassword().equals(password);
  }

  public boolean addUser(String username, String password, Set<AuthType> auths) {
    UserMeta user = new UserMeta(username, password, UserType.OrdinaryUser, auths);
    return metaManager.addUser(user);
  }

  public boolean updateUser(String username, String password, Set<AuthType> auths) {
    return metaManager.updateUser(username, password, auths);
  }

  public boolean deleteUser(String username) {
    UserMeta user = getUser(username);
    if (user == null || user.getUserType() == UserType.Administrator) {
      return false;
    }
    return metaManager.removeUser(username);
  }

  public UserMeta getUser(String username) {
    return metaManager.getUser(username);
  }

  public List<UserMeta> getUsers(List<String> usernames) {
    return metaManager.getUsers(usernames);
  }

  public List<UserMeta> getUsers() {
    return metaManager.getUsers();
  }
}
