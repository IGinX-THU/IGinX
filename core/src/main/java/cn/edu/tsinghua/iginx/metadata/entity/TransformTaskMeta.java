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
package cn.edu.tsinghua.iginx.metadata.entity;

import cn.edu.tsinghua.iginx.thrift.UDFType;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class TransformTaskMeta {

  private String name;

  private String className;

  private String fileName;

  private Set<Pair<String, Integer>> ipPortSet;

  private UDFType type;

  public TransformTaskMeta(
      String name, String className, String fileName, String ip, int port, UDFType type) {
    this(
        name,
        className,
        fileName,
        new HashSet<>(Collections.singleton(new Pair<>(ip, port))),
        type);
  }

  public TransformTaskMeta(
      String name,
      String className,
      String fileName,
      Set<Pair<String, Integer>> ipPortSet,
      UDFType type) {
    this.name = name;
    this.className = className;
    this.fileName = fileName;
    this.ipPortSet = ipPortSet;
    this.type = type;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getClassName() {
    return className;
  }

  public void setClassName(String className) {
    this.className = className;
  }

  public String getFileName() {
    return fileName;
  }

  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  public Set<Pair<String, Integer>> getIpPortSet() {
    return ipPortSet;
  }

  public void setIpPortSet(Set<Pair<String, Integer>> ipPortSet) {
    this.ipPortSet = ipPortSet;
  }

  public void addIpPort(Pair<String, Integer> ipPort) {
    this.ipPortSet.add(ipPort);
  }

  public void addIpPort(String ip, int port) {
    this.ipPortSet.add(new Pair<>(ip, port));
  }

  public boolean containsIpPort(String ip, int port) {
    for (Pair<String, Integer> pair : this.ipPortSet) {
      if (ip.equals(pair.k) && port == pair.v) {
        return true;
      }
    }
    return false;
  }

  public UDFType getType() {
    return type;
  }

  public void setType(UDFType type) {
    this.type = type;
  }

  public TransformTaskMeta copy() {
    return new TransformTaskMeta(name, className, fileName, ipPortSet, type);
  }

  @Override
  public String toString() {
    return "TransformTaskMeta{"
        + "name='"
        + name
        + '\''
        + ", className='"
        + className
        + '\''
        + ", fileName='"
        + fileName
        + '\''
        + ", ip_port='"
        + ipPortSet
        + '\''
        + ", type="
        + type
        + '}';
  }
}
