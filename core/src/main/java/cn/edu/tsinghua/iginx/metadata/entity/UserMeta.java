package cn.edu.tsinghua.iginx.metadata.entity;

import cn.edu.tsinghua.iginx.thrift.AuthType;
import cn.edu.tsinghua.iginx.thrift.UserType;
import java.util.HashSet;
import java.util.Set;

public class UserMeta {

  private String username;

  private String password;

  private UserType userType;

  private Set<AuthType> auths;

  public UserMeta(String username, String password, UserType userType, Set<AuthType> auths) {
    this.username = username;
    this.password = password;
    this.userType = userType;
    this.auths = auths;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public UserType getUserType() {
    return userType;
  }

  public void setUserType(UserType userType) {
    this.userType = userType;
  }

  public Set<AuthType> getAuths() {
    return auths;
  }

  public void setAuths(Set<AuthType> auths) {
    this.auths = auths;
  }

  public UserMeta copy() {
    return new UserMeta(username, password, userType, new HashSet<>(auths));
  }
}
