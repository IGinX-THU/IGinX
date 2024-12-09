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
package cn.edu.tsinghua.iginx.sql.statement;

public enum StatementType {
  NULL,
  SELECT,
  INSERT,
  DELETE,
  INSERT_FROM_SELECT,
  INSERT_FROM_CSV,
  EXPORT_CSV_FROM_SELECT,
  EXPORT_STREAM_FROM_SELECT,
  ADD_STORAGE_ENGINE,
  ALTER_STORAGE_ENGINE,
  SHOW_REPLICATION,
  COUNT_POINTS,
  CLEAR_DATA,
  DELETE_COLUMNS,
  SHOW_COLUMNS,
  SHOW_CLUSTER_INFO,
  CREATE_USER,
  GRANT_USER,
  CHANGE_USER_PASSWORD,
  DROP_USER,
  SHOW_USER,
  SHOW_REGISTER_TASK,
  REGISTER_TASK,
  DROP_TASK,
  COMMIT_TRANSFORM_JOB,
  SHOW_JOB_STATUS,
  CANCEL_JOB,
  SHOW_ELIGIBLE_JOB,
  REMOVE_HISTORY_DATA_SOURCE,
  SET_CONFIG,
  SHOW_CONFIG,
  SHOW_SESSION_ID,
  COMPACT,
  SHOW_RULES,
  SET_RULES
}
