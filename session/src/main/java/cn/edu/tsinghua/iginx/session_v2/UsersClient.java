package cn.edu.tsinghua.iginx.session_v2;

import cn.edu.tsinghua.iginx.session_v2.domain.User;
import cn.edu.tsinghua.iginx.session_v2.exception.IginXException;
import java.util.List;

public interface UsersClient {

  void addUser(final User user) throws IginXException;

  void updateUser(final User user) throws IginXException;

  void updateUserPassword(final String username, final String newPassword);

  void removeUser(final String username) throws IginXException;

  User findUserByName(final String username) throws IginXException;

  List<User> findUsers() throws IginXException;
}
