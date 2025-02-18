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
package cn.edu.tsinghua.iginx.utils;

import java.util.List;
import java.util.Objects;

public class EmailFromYAML {

  private String hostName;
  private String smtpPort = "465";
  private String userName;
  private String password;
  private String from;
  private List<String> to;

  public String getHostName() {
    return hostName;
  }

  public void setHostName(String hostName) {
    this.hostName = hostName;
  }

  public String getSmtpPort() {
    return smtpPort;
  }

  public void setSmtpPort(String smtpPort) {
    this.smtpPort = smtpPort;
  }

  public String getUserName() {
    return userName;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public String getFrom() {
    return from;
  }

  public void setFrom(String from) {
    this.from = from;
  }

  public List<String> getTo() {
    return to;
  }

  public void setTo(List<String> to) {
    this.to = to;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    EmailFromYAML that = (EmailFromYAML) o;
    return Objects.equals(hostName, that.hostName)
        && Objects.equals(smtpPort, that.smtpPort)
        && Objects.equals(userName, that.userName)
        && Objects.equals(password, that.password)
        && Objects.equals(from, that.from)
        && Objects.equals(to, that.to);
  }

  @Override
  public int hashCode() {
    return Objects.hash(hostName, smtpPort, userName, password, from, to);
  }

  @Override
  public String toString() {
    return "EmailFromYAML{"
        + "hostName='"
        + hostName
        + '\''
        + ", smtpPort='"
        + smtpPort
        + '\''
        + ", userName='"
        + userName
        + '\''
        + ", password='"
        + password
        + '\''
        + ", from='"
        + from
        + '\''
        + ", to="
        + to
        + '}';
  }
}
