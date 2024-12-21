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

import static org.junit.Assert.*;

import cn.edu.tsinghua.iginx.thrift.JobState;
import cn.edu.tsinghua.iginx.transform.pojo.Job;
import cn.edu.tsinghua.iginx.utils.JobFromYAML;
import com.icegreen.greenmail.junit4.GreenMailRule;
import com.icegreen.greenmail.util.GreenMailUtil;
import com.icegreen.greenmail.util.ServerSetupTest;
import java.util.Arrays;
import java.util.Collections;
import javax.mail.MessagingException;
import javax.mail.internet.MimeMessage;
import org.apache.commons.mail.EmailException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmailNotifierTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(EmailNotifierTest.class);

  @Rule public final GreenMailRule greenMail = new GreenMailRule(ServerSetupTest.SMTPS);

  EmailNotifier emailNotifier;

  @Before
  public void before() {
    greenMail.setUser("from@localhost", "password");
    emailNotifier =
        new EmailNotifier(
            "localhost",
            "3465",
            "from@localhost",
            "password",
            "from@localhost",
            Collections.singletonList("to@localhost"));
  }

  @Test
  public void testSendEmail() throws MessagingException, EmailException {
    emailNotifier.sendEmail("subject", "body");

    Assert.assertTrue(greenMail.waitForIncomingEmail(10 * 1000, 1));

    assertEquals(1, greenMail.getReceivedMessages().length);
    MimeMessage mimeMessage = greenMail.getReceivedMessages()[0];

    assertEquals("subject", mimeMessage.getSubject());
    assertEquals("body", GreenMailUtil.getBody(mimeMessage));
    assertEquals("[from@localhost]", Arrays.toString(mimeMessage.getFrom()));
    assertEquals("[to@localhost]", Arrays.toString(mimeMessage.getAllRecipients()));
  }

  @Test
  public void testNotifyJobState() throws MessagingException, EmailException {
    JobFromYAML jobFromYAML = new JobFromYAML();
    jobFromYAML.setExportType("csv");
    jobFromYAML.setTaskList(Collections.emptyList());
    Job job = new Job(53, 102, jobFromYAML);
    job.setStartTime(1716384072742L);
    job.setState(JobState.JOB_FINISHED);
    job.setEndTime(1716384072743L);
    emailNotifier.send(job);

    Assert.assertTrue(greenMail.waitForIncomingEmail(10 * 1000, 1));

    assertEquals(1, greenMail.getReceivedMessages().length);
    MimeMessage mimeMessage = greenMail.getReceivedMessages()[0];

    assertEquals("Job 53 is finished", mimeMessage.getSubject());
  }

  @Test
  public void testNotifyJobStateException() throws MessagingException, EmailException {
    JobFromYAML jobFromYAML = new JobFromYAML();
    jobFromYAML.setExportType("csv");
    jobFromYAML.setTaskList(Collections.emptyList());
    Job job = new Job(53, 102, jobFromYAML);
    try {
      throw new Exception("example exception");
    } catch (Exception e) {
      job.setException(e);
    }
    job.setStartTime(1716384072742L);
    job.setState(JobState.JOB_FINISHED);
    job.setEndTime(1716384072743L);
    emailNotifier.send(job);

    Assert.assertTrue(greenMail.waitForIncomingEmail(10 * 1000, 1));

    assertEquals(1, greenMail.getReceivedMessages().length);
    MimeMessage mimeMessage = greenMail.getReceivedMessages()[0];

    LOGGER.info(GreenMailUtil.getBody(mimeMessage));

    assertEquals("Job 53 is finished", mimeMessage.getSubject());
    assertTrue(GreenMailUtil.getBody(mimeMessage).contains("example exception"));
  }
}
