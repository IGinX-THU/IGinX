package cn.edu.tsinghua.iginx.notice;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.thrift.JobState;
import cn.edu.tsinghua.iginx.transform.pojo.Job;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Date;
import org.apache.commons.mail.Email;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.SimpleEmail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmailNotifier {
  private static final Logger LOGGER = LoggerFactory.getLogger(EmailNotifier.class);
  private static final EmailNotifier INSTANCE = new EmailNotifier();

  boolean mailEnable;
  private final String mailHost;
  private final int mailPort;
  private final String mailUsername;
  private final String mailPassword;
  private final String mailSender;
  private final String recipients;
  private final String iginxHost;
  private final int iginxPort;

  public static EmailNotifier getInstance() {
    return INSTANCE;
  }

  public EmailNotifier() {
    this(ConfigDescriptor.getInstance().getConfig());
  }

  public EmailNotifier(Config config) {
    this(
        config.isEnableEmailNotification(),
        config.getMailSmtpHost(),
        config.getMailSmtpPort(),
        config.getMailSmtpUser(),
        config.getMailSmtpPassword(),
        config.getMailSender(),
        config.getMailRecipient(),
        config.getIp(),
        config.getPort());
  }

  public EmailNotifier(
      boolean mailEnable,
      String mailHost,
      int mailPort,
      String mailUsername,
      String mailPassword,
      String mailSender,
      String mailRecipient,
      String iginxHost,
      int iginxPort) {
    this.mailEnable = mailEnable;
    this.mailHost = mailHost;
    this.mailPort = mailPort;
    this.mailUsername = mailUsername;
    this.mailPassword = mailPassword;
    this.mailSender = mailSender;
    this.recipients = mailRecipient;
    this.iginxHost = iginxHost;
    this.iginxPort = iginxPort;
  }

  void sendEmail(String subject, String content) {
    if (!mailEnable) {
      LOGGER.debug("Email notification is disabled. Subject: {}, Content: {}", subject, content);
      return;
    }

    try {
      Email email = new SimpleEmail();
      email.setHostName(mailHost);
      email.setSslSmtpPort(String.valueOf(mailPort));
      email.setSSLOnConnect(true);
      email.setAuthentication(mailUsername, mailPassword);
      email.setFrom(mailSender);
      email.setSubject(subject);
      email.setMsg(content);
      email.addTo(recipients.split(","));
      email.send();
      LOGGER.info("Email notification sent. Subject: {}", subject);
    } catch (EmailException e) {
      LOGGER.error("Failed to send email notification. Subject: {}", subject, e);
    }
  }

  public void send(Job job) {
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
    content.append("IGinX Host: ").append(iginxHost).append("\n");
    content.append("IGinX Port: ").append(iginxPort).append("\n");

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
}
