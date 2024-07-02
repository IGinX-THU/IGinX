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
package cn.edu.tsinghua.iginx.migration.recover;

import cn.edu.tsinghua.iginx.migration.MigrationTask;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MigrationLogger {

  private static final Logger LOGGER = LoggerFactory.getLogger(MigrationLogger.class);

  public static final String MIGRATION_LOG_NAME = "migration.log";
  public static final String SOURCE_NAME = "migration-tasks";
  public static final String MIGRATION_EXECUTE_TASK_START = "migration execute task start";
  public static final String MIGRATION_EXECUTE_TASK_END = "migration execute task end";
  public static final String MIGRATION_FINISHED = "migration finished";

  private BufferedWriter logStream;

  public MigrationLogger() {
    try {
      logStream = new BufferedWriter(new FileWriter(MIGRATION_LOG_NAME));
    } catch (IOException e) {
      LOGGER.error("create log stream failed ", e);
    }
  }

  public void close() throws IOException {
    logStream.close();
  }

  public void logMigrationTasks(List<MigrationTask> migrationTasks) {
    try {
      logStream.write(SOURCE_NAME);
      logStream.newLine();
      for (MigrationTask migrationTask : migrationTasks) {
        logStream.write(migrationTask.toString());
        logStream.newLine();
      }
      logStream.flush();
    } catch (IOException e) {
      LOGGER.error("write log failed ", e);
    }
  }

  public void logFinish() {
    try {
      logStream.write(MIGRATION_FINISHED);
      logStream.flush();
    } catch (IOException e) {
      LOGGER.error("write log failed ", e);
    }
  }

  public void logMigrationExecuteTaskStart(MigrationExecuteTask migrationExecuteTask) {
    try {
      logStream.write(MIGRATION_EXECUTE_TASK_START);
      logStream.newLine();
      logStream.write(migrationExecuteTask.toString());
      logStream.newLine();
      logStream.flush();
    } catch (IOException e) {
      LOGGER.error("write log failed ", e);
    }
  }

  public void logMigrationExecuteTaskEnd() {
    try {
      logStream.write(MIGRATION_EXECUTE_TASK_END);
      logStream.newLine();
      logStream.flush();
    } catch (IOException e) {
      LOGGER.error("write log failed ", e);
    }
  }
}
