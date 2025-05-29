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
package cn.edu.tsinghua.iginx.transform.exec.tools;

import cn.edu.tsinghua.iginx.engine.shared.function.manager.ThreadInterpreterManager;
import cn.edu.tsinghua.iginx.transform.driver.PemjaDriver;
import cn.edu.tsinghua.iginx.transform.pojo.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobListener;

public class TransformJobListener implements JobListener {
  @Override
  public String getName() {
    return "JobExceptionListener";
  }

  @Override
  public void jobToBeExecuted(JobExecutionContext context) {
    if (!ThreadInterpreterManager.isConfigSet()) {
      // will only execute once
      ThreadInterpreterManager.setConfig(PemjaDriver.getPythonConfig());
    }
  }

  @Override
  public void jobExecutionVetoed(JobExecutionContext context) {}

  @Override
  public void jobWasExecuted(JobExecutionContext context, JobExecutionException jobException) {
    if (jobException != null) {
      Job job = (Job) context.getMergedJobDataMap().get("job");
      job.setException(jobException);
    }
  }
}
