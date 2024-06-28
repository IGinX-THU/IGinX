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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package cn.edu.tsinghua.iginx.relational.tools;

import java.util.HashMap;
import java.util.Map;

public abstract class Constants {
  public static final String TAGKV_EQUAL = "=";

  public static final String TAGKV_SEPARATOR = "-";

  public static final int BATCH_SIZE = 10000;

  public static final String USERNAME = "username";

  public static final String PASSWORD = "password";

  public static final String KEY_NAME = "RELATIONAL+KEY";

  public static final String DATABASE_PREFIX = "unit";

  public static final String CREATE_DATABASE_STATEMENT = "CREATE DATABASE %s;";

  public static final String QUERY_STATEMENT_WITHOUT_KEYNAME = "SELECT %s FROM %s %s ORDER BY %s;";

  public static final String ADD_COLUMN_STATEMENT = "ALTER TABLE %s ADD COLUMN %s %s;";

  public static final String DROP_COLUMN_STATEMENT = "ALTER TABLE %s DROP COLUMN %s;";

  public static final Map<String, String> classMap = new HashMap<>();

  static {
    classMap.put("postgresql", "cn.edu.tsinghua.iginx.relational.meta.PostgreSQLMeta");
  }
}
