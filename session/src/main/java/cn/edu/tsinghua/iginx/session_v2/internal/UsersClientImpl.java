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
package cn.edu.tsinghua.iginx.session_v2.internal;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.session_v2.Arguments;
import cn.edu.tsinghua.iginx.session_v2.UsersClient;
import cn.edu.tsinghua.iginx.session_v2.domain.User;
import cn.edu.tsinghua.iginx.session_v2.exception.IginXException;
import cn.edu.tsinghua.iginx.thrift.AddUserReq;
import cn.edu.tsinghua.iginx.thrift.AuthType;
import cn.edu.tsinghua.iginx.thrift.DeleteUserReq;
import cn.edu.tsinghua.iginx.thrift.GetUserReq;
import cn.edu.tsinghua.iginx.thrift.GetUserResp;
import cn.edu.tsinghua.iginx.thrift.Status;
import cn.edu.tsinghua.iginx.thrift.UpdateUserReq;
import cn.edu.tsinghua.iginx.thrift.UserType;
import cn.edu.tsinghua.iginx.utils.StatusUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.thrift.TException;

public class UsersClientImpl extends AbstractFunctionClient implements UsersClient {

  public UsersClientImpl(IginXClientImpl iginXClient) {
    super(iginXClient);
  }

  @Override
  public void addUser(User user) throws IginXException {
    Arguments.checkNotNull(user, "user");
    Arguments.checkNotNull(user.getUsername(), "username");
    Arguments.checkNotNull(user.getPassword(), "password");
    Arguments.checkNotNull(user.getAuths(), "auths");

    AddUserReq req =
        new AddUserReq(sessionId, user.getUsername(), user.getPassword(), user.getAuths());

    synchronized (iginXClient) {
      iginXClient.checkIsClosed();
      try {
        Status status = client.addUser(req);
        StatusUtils.verifySuccess(status);
      } catch (TException | SessionException e) {
        throw new IginXException("add user failure: ", e);
      }
    }
  }

  @Override
  public void updateUser(User user) throws IginXException {
    Arguments.checkNotNull(user, "user");
    Arguments.checkNotNull(user.getUsername(), "username");

    UpdateUserReq req = new UpdateUserReq(sessionId, user.getUsername());

    if (user.getPassword() != null) {
      req.setPassword(req.getPassword());
    }
    if (user.getAuths() != null) {
      req.setAuths(user.getAuths());
    }

    synchronized (iginXClient) {
      iginXClient.checkIsClosed();
      try {
        Status status = client.updateUser(req);
        StatusUtils.verifySuccess(status);
      } catch (TException | SessionException e) {
        throw new IginXException("update user failure: ", e);
      }
    }
  }

  @Override
  public void updateUserPassword(String username, String newPassword) {
    Arguments.checkNotNull(username, "username");
    Arguments.checkNotNull(newPassword, "newPassword");

    UpdateUserReq req = new UpdateUserReq(sessionId, username);
    req.setPassword(newPassword);

    synchronized (iginXClient) {
      iginXClient.checkIsClosed();
      try {
        Status status = client.updateUser(req);
        StatusUtils.verifySuccess(status);
      } catch (TException | SessionException e) {
        throw new IginXException("update user failure: ", e);
      }
    }
  }

  @Override
  public void removeUser(String username) throws IginXException {
    Arguments.checkNotNull(username, "username");

    DeleteUserReq req = new DeleteUserReq(sessionId, username);
    synchronized (iginXClient) {
      iginXClient.checkIsClosed();
      try {
        Status status = client.deleteUser(req);
        StatusUtils.verifySuccess(status);
      } catch (TException | SessionException e) {
        throw new IginXException("Remove user failure: ", e);
      }
    }
  }

  @Override
  public User findUserByName(String username) throws IginXException {
    Arguments.checkNotNull(username, "username");

    GetUserReq req = new GetUserReq(sessionId);
    req.setUsernames(Collections.singletonList(username));

    GetUserResp resp;

    synchronized (iginXClient) {
      iginXClient.checkIsClosed();
      try {
        resp = client.getUser(req);
        StatusUtils.verifySuccess(resp.status);
      } catch (TException | SessionException e) {
        throw new IginXException("find user failure: ", e);
      }
    }

    if (resp.usernames == null || resp.usernames.isEmpty()) {
      return null;
    }
    UserType userType = resp.userTypes.get(0);
    Set<AuthType> auths = resp.auths.get(0);

    return new User(username, userType, auths);
  }

  @Override
  public List<User> findUsers() throws IginXException {
    GetUserReq req = new GetUserReq(sessionId);

    GetUserResp resp;

    synchronized (iginXClient) {
      iginXClient.checkIsClosed();
      try {
        resp = client.getUser(req);
        StatusUtils.verifySuccess(resp.status);
      } catch (TException | SessionException e) {
        throw new IginXException("find users failure: ", e);
      }
    }

    if (resp.usernames == null || resp.userTypes == null || resp.auths == null) {
      return Collections.emptyList();
    }

    List<User> users = new ArrayList<>();
    for (int i = 0; i < resp.usernames.size(); i++) {
      String username = resp.usernames.get(i);
      UserType userType = resp.userTypes.get(i);
      Set<AuthType> auths = resp.auths.get(i);
      users.add(new User(username, userType, auths));
    }

    return users;
  }
}
