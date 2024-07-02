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
