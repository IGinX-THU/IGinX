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
package cn.edu.tsinghua.iginx.notice;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.thrift.JobState;
import cn.edu.tsinghua.iginx.transform.pojo.Job;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Date;
import java.util.List;
import org.apache.commons.mail.Email;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.SimpleEmail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmailNotifier {
  private static final Logger LOGGER = LoggerFactory.getLogger(EmailNotifier.class);

  private final String hostName;
  private final String smtpPort;
  private final String username;
  private final String password;
  private final String from;
  private final List<String> to;

  static {
    System.setProperty("mail.smtp.ssl.trust", "*");
  }

  public EmailNotifier(
      String hostName,
      String smtpPort,
      String username,
      String password,
      String from,
      List<String> to) {
    this.hostName = hostName;
    this.smtpPort = smtpPort;
    this.username = username;
    this.password = password;
    this.from = from;
    this.to = to;
  }

  public void sendEmail(String subject, String content) throws EmailException {
    LOGGER.debug("Send email notification, Subject: {}, Content: {}", subject, content);

    Email email = new SimpleEmail();
    email.setSSLOnConnect(true);
    email.setStartTLSEnabled(true);
    if (hostName != null) {
      email.setHostName(hostName);
    }
    if (smtpPort != null) {
      email.setStartTLSEnabled(true);
      email.setSslSmtpPort(smtpPort);
    }
    if (username != null && password != null) {
      email.setAuthentication(username, password);
    }
    if (from != null) {
      email.setFrom(from);
    }
    email.setSubject(subject);
    email.setMsg(content);
    if (to != null) {
      email.addTo(to.toArray(new String[0]));
    }
    email.send();
    LOGGER.info("Email notification sent. Subject: {}", subject);
  }

  public void send(Job job) throws EmailException {
    JobState jobState = job.getState();
    String jobStateStr = jobState.name().split("_")[1].toLowerCase();
    String subject = String.format("Job %d is %s", job.getJobId(), jobStateStr);

    StringBuilder content = new StringBuilder();
    content.append("Job ID: ").append(job.getJobId()).append("\n");
    content.append("Job State: ").append(jobStateStr).append("\n");
    content.append("Job Start Time: ").append(new Date(job.getStartTime())).append("\n");
    long endTime = job.getEndTime();
    if (endTime != 0) {
      content.append("Job End Time: ").append(new Date(endTime)).append("\n");
    }
    content
        .append("IGinX Host: ")
        .append(ConfigDescriptor.getInstance().getConfig().getIp())
        .append("\n");
    content
        .append("IGinX Port: ")
        .append(ConfigDescriptor.getInstance().getConfig().getPort())
        .append("\n");

    Exception e = job.getException();
    if (e != null) {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      e.printStackTrace(pw);
      content.append("Exception: ").append("\n");
      content.append(sw);
    }

    sendEmail(subject, content.toString());
  }

  @Override
  public String toString() {
    return "EmailNotifier{"
        + "hostname='"
        + hostName
        + '\''
        + ", smtpPort="
        + smtpPort
        + ", username='"
        + username
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
