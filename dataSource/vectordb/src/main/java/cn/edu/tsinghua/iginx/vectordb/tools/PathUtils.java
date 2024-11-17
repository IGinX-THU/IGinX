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
package cn.edu.tsinghua.iginx.vectordb.tools;

import static cn.edu.tsinghua.iginx.vectordb.tools.Constants.QUOTA;

import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import cn.edu.tsinghua.iginx.vectordb.support.PathSystem;
import cn.edu.tsinghua.iginx.vectordb.support.impl.MilvusPathSystem;
import io.milvus.v2.client.MilvusClientV2;
import java.util.List;
import java.util.Map;

public class PathUtils {

  public static PathSystem pathSystem = null;

  public static PathSystem dummyPathSystem = null;

  public static void resetPathSystem() {
    pathSystem = null;
  }

  public static PathSystem getPathSystem(MilvusClientV2 client) {
    if (pathSystem == null) {
      init(client);
    }
    return pathSystem;
  }

  //    public static PathSystem getDummyPathSystem() {
  //        if (dummyPathSystem == null){
  //            initDummy();
  //        }
  //        return dummyPathSystem;
  //    }

  public static synchronized void init(MilvusClientV2 client) {
    if (pathSystem != null) return;
    pathSystem = new MilvusPathSystem();
    for (String databaseName : MilvusClientUtils.listDatabase(client)) {
      for (String collectionName : MilvusClientUtils.listCollections(client, databaseName)) {
        Map<String, DataType> paths =
            MilvusClientUtils.getCollectionPaths(client, databaseName, collectionName);
        paths
            .keySet()
            .forEach(
                path ->
                    pathSystem.addPath(
                        path, MilvusClientUtils.isDummy(databaseName), paths.get(path)));
      }
    }
  }

  //    public static synchronized void initDummy(){
  //        if (dummyPathSystem != null) return;
  //        dummyPathSystem = new MilvusPathSystem();
  //    }

  public static String getPathEscaped(
      String databaseName, String collectionName, String fieldName) {
    StringBuilder path = new StringBuilder();
    if (!databaseName.startsWith(Constants.DATABASE_PREFIX)) {
      path.append(NameUtils.unescape(databaseName)).append(Constants.PATH_SEPARATOR);
    }
    return path.append(NameUtils.unescape(collectionName))
        .append(Constants.PATH_SEPARATOR)
        .append(NameUtils.unescape(fieldName))
        .toString();
  }

  public static String getPathUnescaped(
      String databaseName, String collectionName, String fieldName) {
    StringBuilder path = new StringBuilder();
    if (!databaseName.startsWith(Constants.DATABASE_PREFIX)) {
      path.append(databaseName).append(Constants.PATH_SEPARATOR);
    }
    return path.append(collectionName)
        .append(Constants.PATH_SEPARATOR)
        .append(fieldName)
        .toString();
  }

  public static String findPath(String path, Map<String, String> tags) {
    if (pathSystem == null) {
      return null;
    }
    return pathSystem.findPath(path, tags);
  }

  public static Pair<String, String> getCollectionAndFieldByPath(
      String path, Map<String, String> tags, boolean isDummy) {
    VectorDBSchema schema = new VectorDBSchema(path, isDummy, QUOTA);
    String collectionName = schema.getCollectionName();
    String fieldName = schema.getFieldName();
    fieldName = TagKVUtils.toFullName(fieldName, tags);
    return new Pair<>(collectionName, fieldName);
    //        return new Pair<>(NameUtils.escape(collectionName),NameUtils.escape(fieldName));
  }

  public static boolean match(String path, List<String> patterns) {
    for (String pattern : patterns) {
      if (path.equals(pattern)) {
        return true;
      } else if (path.contains("*") && pattern.matches(StringUtils.reformatPath(path))) {
        return true;
      } else if (pattern.contains("*") && path.matches(StringUtils.reformatPath(pattern))) {
        return true;
      } else if (path.matches(StringUtils.toRegexExpr(pattern + ".*"))) {
        return true;
      }
    }
    return false;
  }
}
