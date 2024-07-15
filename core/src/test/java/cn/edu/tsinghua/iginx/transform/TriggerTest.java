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
package cn.edu.tsinghua.iginx.transform;

import static org.junit.Assert.*;

import cn.edu.tsinghua.iginx.transform.pojo.JobScheduleTriggerMaker;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.quartz.CronTrigger;
import org.quartz.DateBuilder;
import org.quartz.Trigger;
import org.quartz.impl.triggers.CalendarIntervalTriggerImpl;
import org.quartz.impl.triggers.SimpleTriggerImpl;

public class TriggerTest {

  private static final SimpleDateFormat DATE_TIME_FORMAT =
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  private static final SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("HH:mm:ss");

  private Trigger make(String schedule) {
    return JobScheduleTriggerMaker.getTrigger(schedule);
  }

  @Test
  public void testEveryTrigger() {
    String schedule = "every 3 second";
    Trigger trigger = make(schedule);
    assertNotNull(trigger);
    assertTrue(trigger instanceof SimpleTriggerImpl);
    assertEquals(3000L, ((SimpleTriggerImpl) trigger).getRepeatInterval());

    // minute
    schedule = "every 10 minute";
    trigger = make(schedule);
    assertNotNull(trigger);
    assertTrue(trigger instanceof SimpleTriggerImpl);
    assertEquals(600000L, ((SimpleTriggerImpl) trigger).getRepeatInterval());

    // hour
    schedule = "every 2 hour";
    trigger = make(schedule);
    assertNotNull(trigger);
    assertTrue(trigger instanceof SimpleTriggerImpl);
    assertEquals(7200000L, ((SimpleTriggerImpl) trigger).getRepeatInterval());

    // day
    schedule = "every 1 day";
    trigger = make(schedule);
    assertNotNull(trigger);
    assertTrue(trigger instanceof SimpleTriggerImpl);
    assertEquals(86400000L, ((SimpleTriggerImpl) trigger).getRepeatInterval());

    // month
    schedule = "every 1 month";
    trigger = make(schedule);
    assertNotNull(trigger);
    assertTrue(trigger instanceof CalendarIntervalTriggerImpl);
    assertEquals(1, ((CalendarIntervalTriggerImpl) trigger).getRepeatInterval());
    assertEquals(
        DateBuilder.IntervalUnit.MONTH,
        ((CalendarIntervalTriggerImpl) trigger).getRepeatIntervalUnit());

    // year
    schedule = "every 1 year";
    trigger = make(schedule);
    assertNotNull(trigger);
    assertTrue(trigger instanceof CalendarIntervalTriggerImpl);
    assertEquals(1, ((CalendarIntervalTriggerImpl) trigger).getRepeatInterval());
    assertEquals(
        DateBuilder.IntervalUnit.YEAR,
        ((CalendarIntervalTriggerImpl) trigger).getRepeatIntervalUnit());
  }

  @Test
  public void testEveryTriggerWithDateTimeBounds() throws ParseException {
    String schedule = "every 10 minute starts '2099-02-03 12:00:00' ends '2099-02-04 12:00:00'";
    Date expectedStartTime = DATE_TIME_FORMAT.parse("2099-02-03 12:00:00");
    Date expectedEndTime = DATE_TIME_FORMAT.parse("2099-02-04 12:00:00");

    Trigger trigger = make(schedule);
    assertNotNull(trigger);
    assertTrue(trigger instanceof SimpleTriggerImpl);
    assertEquals(600000L, ((SimpleTriggerImpl) trigger).getRepeatInterval());
    assertEquals(expectedStartTime, trigger.getStartTime());
    assertEquals(expectedEndTime, trigger.getEndTime());
  }

  @Test
  public void testEveryTriggerWithTimeBounds() throws ParseException, InterruptedException {
    String schedule = "every 5 second starts '23:59:53' ends '23:59:59'";

    Calendar now = Calendar.getInstance();
    String today = new SimpleDateFormat("yyyy-MM-dd").format(new Date());

    // wait till next day if startTime-3s is before current time, -3s to make sure we have enough
    // time
    // +5 to make sure it's next day
    if (now.after(DATE_TIME_FORMAT.parse(today + " " + "23:59:50"))) {
      long toNextDay = 24 * 60 * 60 * 1000 - now.getTimeInMillis() + 5;
      TimeUnit.MILLISECONDS.sleep(toNextDay);
    }
    today = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
    Date expectedStartTime = DATE_TIME_FORMAT.parse(today + " " + "23:59:53");
    Date expectedEndTime = DATE_TIME_FORMAT.parse(today + " " + "23:59:59");

    Trigger trigger = make(schedule);
    assertNotNull(trigger);
    assertTrue(trigger instanceof SimpleTriggerImpl);
    assertEquals(5000L, ((SimpleTriggerImpl) trigger).getRepeatInterval());
    assertEquals(expectedStartTime, trigger.getStartTime());
    assertEquals(expectedEndTime, trigger.getEndTime());
  }

  @Test
  public void testEveryTriggerWithStartDateTimeBound() throws ParseException, InterruptedException {
    // with date
    String schedule = "every 10 minute starts '2099-02-03 12:00:00'";
    Date expectedStartTime = DATE_TIME_FORMAT.parse("2099-02-03 12:00:00");

    Trigger trigger = make(schedule);
    assertNotNull(trigger);
    assertTrue(trigger instanceof SimpleTriggerImpl);
    assertEquals(600000L, ((SimpleTriggerImpl) trigger).getRepeatInterval());
    assertEquals(expectedStartTime, trigger.getStartTime());
    assertNull(trigger.getEndTime());

    // no date, may need to wait till next day
    schedule = "every 5 second starts '23:59:58'";
    Calendar now = Calendar.getInstance();
    String today = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
    if (now.after(DATE_TIME_FORMAT.parse(today + " 23:59:55"))) {
      long toNextDay = 24 * 60 * 60 * 1000 - now.getTimeInMillis() + 5;
      TimeUnit.MILLISECONDS.sleep(toNextDay);
    }
    today = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
    expectedStartTime = DATE_TIME_FORMAT.parse(today + " " + "23:59:58");

    trigger = make(schedule);
    assertNotNull(trigger);
    assertTrue(trigger instanceof SimpleTriggerImpl);
    assertEquals(5000L, ((SimpleTriggerImpl) trigger).getRepeatInterval());
    assertEquals(expectedStartTime, trigger.getStartTime());
    assertNull(trigger.getEndTime());
  }

  @Test
  public void testEveryTriggerWithEndDateTimeBound() throws ParseException, InterruptedException {
    // with date
    String schedule = "every 10 minute ends '2099-02-03 12:00:00'";
    Date expectedEndTime = DATE_TIME_FORMAT.parse("2099-02-03 12:00:00");

    Trigger trigger = make(schedule);
    assertNotNull(trigger);
    assertTrue(trigger instanceof SimpleTriggerImpl);
    assertEquals(600000L, ((SimpleTriggerImpl) trigger).getRepeatInterval());
    assertEquals(expectedEndTime, trigger.getEndTime());

    long currentTime = System.currentTimeMillis();
    long triggerStartTime = trigger.getStartTime().getTime();

    // Allow a tolerance of 1000 milliseconds, 500 would fail
    long tolerance = 1000L;

    assertTrue(
        "The trigger start time is not within the expected tolerance range.",
        Math.abs(triggerStartTime - currentTime) <= tolerance);

    // no date, may need to wait till next day
    schedule = "every 5 second ends '23:59:57'";
    Calendar now = Calendar.getInstance();
    String today = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
    if (now.after(DATE_TIME_FORMAT.parse(today + " 23:59:57"))) {
      long toNextDay = 24 * 60 * 60 * 1000 - now.getTimeInMillis() + 5;
      TimeUnit.MILLISECONDS.sleep(toNextDay);
    }
    today = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
    expectedEndTime = DATE_TIME_FORMAT.parse(today + " " + "23:59:57");

    trigger = make(schedule);
    assertNotNull(trigger);
    assertTrue(trigger instanceof SimpleTriggerImpl);
    assertEquals(5000L, ((SimpleTriggerImpl) trigger).getRepeatInterval());
    assertEquals(expectedEndTime, trigger.getEndTime());

    currentTime = System.currentTimeMillis();
    triggerStartTime = trigger.getStartTime().getTime();

    assertTrue(
        "The trigger start time is not within the expected tolerance range.",
        Math.abs(triggerStartTime - currentTime) <= tolerance);
  }

  @Test
  public void testAfterTrigger() {
    // second
    String schedule = "after 3 second";
    long currentTime = System.currentTimeMillis();
    Trigger trigger = make(schedule);
    assertNotNull(trigger);
    assertTrue(trigger instanceof SimpleTriggerImpl);
    long triggerStartTime = trigger.getStartTime().getTime();
    long expectedStartTime = currentTime + 3000L;

    // Allow a tolerance of 1000 milliseconds
    long tolerance = 1000L;
    assertTrue(
        "The trigger start time is not within the expected tolerance range.",
        Math.abs(triggerStartTime - expectedStartTime) <= tolerance);

    // minute
    schedule = "after 3 minute";
    currentTime = System.currentTimeMillis();
    trigger = make(schedule);
    assertNotNull(trigger);
    assertTrue(trigger instanceof SimpleTriggerImpl);
    triggerStartTime = trigger.getStartTime().getTime();
    expectedStartTime = currentTime + 3 * 60 * 1000L;
    assertTrue(
        "The trigger start time is not within the expected tolerance range.",
        Math.abs(triggerStartTime - expectedStartTime) <= tolerance);

    // hour
    schedule = "after 3 hour";
    currentTime = System.currentTimeMillis();
    trigger = make(schedule);
    assertNotNull(trigger);
    assertTrue(trigger instanceof SimpleTriggerImpl);
    triggerStartTime = trigger.getStartTime().getTime();
    expectedStartTime = currentTime + 3 * 60 * 60 * 1000L;
    assertTrue(
        "The trigger start time is not within the expected tolerance range.",
        Math.abs(triggerStartTime - expectedStartTime) <= tolerance);

    // day
    schedule = "after 3 day";
    currentTime = System.currentTimeMillis();
    trigger = make(schedule);
    assertNotNull(trigger);
    assertTrue(trigger instanceof SimpleTriggerImpl);
    triggerStartTime = trigger.getStartTime().getTime();
    expectedStartTime = currentTime + 3 * 60 * 60 * 24 * 1000L;
    assertTrue(
        "The trigger start time is not within the expected tolerance range.",
        Math.abs(triggerStartTime - expectedStartTime) <= tolerance);

    // month
    schedule = "after 3 month";
    currentTime = System.currentTimeMillis();
    trigger = make(schedule);
    assertNotNull(trigger);
    assertTrue(trigger instanceof SimpleTriggerImpl);

    // Calculate the expected start time by adding 3 months to the current time
    Calendar expectedStartCalendar = Calendar.getInstance();
    expectedStartCalendar.setTimeInMillis(currentTime);
    int currentHour = expectedStartCalendar.get(Calendar.HOUR_OF_DAY);
    int currentMinute = expectedStartCalendar.get(Calendar.MINUTE);
    int currentSecond = expectedStartCalendar.get(Calendar.SECOND);
    expectedStartCalendar.add(Calendar.MONTH, 3);
    expectedStartCalendar.set(Calendar.HOUR_OF_DAY, currentHour);
    expectedStartCalendar.set(Calendar.MINUTE, currentMinute);
    expectedStartCalendar.set(Calendar.SECOND, currentSecond);
    expectedStartTime = expectedStartCalendar.getTimeInMillis();
    triggerStartTime = trigger.getStartTime().getTime();
    assertTrue(
        "The trigger start time is not within the expected tolerance range.",
        Math.abs(triggerStartTime - expectedStartTime) <= tolerance);

    // year
    schedule = "after 3 year";
    currentTime = System.currentTimeMillis();
    trigger = make(schedule);
    assertNotNull(trigger);
    assertTrue(trigger instanceof SimpleTriggerImpl);

    // Calculate the expected start time by adding 3 years to the current time
    expectedStartCalendar = Calendar.getInstance();
    expectedStartCalendar.setTimeInMillis(currentTime);
    currentHour = expectedStartCalendar.get(Calendar.HOUR_OF_DAY);
    currentMinute = expectedStartCalendar.get(Calendar.MINUTE);
    currentSecond = expectedStartCalendar.get(Calendar.SECOND);
    expectedStartCalendar.add(Calendar.YEAR, 3);
    expectedStartCalendar.set(Calendar.HOUR_OF_DAY, currentHour);
    expectedStartCalendar.set(Calendar.MINUTE, currentMinute);
    expectedStartCalendar.set(Calendar.SECOND, currentSecond);
    expectedStartTime = expectedStartCalendar.getTimeInMillis();
    triggerStartTime = trigger.getStartTime().getTime();
    assertTrue(
        "The trigger start time is not within the expected tolerance range.",
        Math.abs(triggerStartTime - expectedStartTime) <= tolerance);
  }

  @Test
  public void testAtTrigger() throws ParseException, InterruptedException {
    // with date
    String schedule = "at '2099-12-31 23:59:59'";
    Trigger trigger = make(schedule);
    Date atDate = DATE_TIME_FORMAT.parse("2099-12-31 23:59:59");
    assertNotNull(trigger);
    assertTrue(trigger instanceof SimpleTriggerImpl);
    assertEquals(atDate, trigger.getStartTime());

    // no date, may need to wait till next day
    schedule = "at '23:59:57'";
    Calendar now = Calendar.getInstance();
    String today = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
    if (now.after(DATE_TIME_FORMAT.parse(today + " 23:59:57"))) {
      long toNextDay = 24 * 60 * 60 * 1000 - now.getTimeInMillis() + 5;
      TimeUnit.MILLISECONDS.sleep(toNextDay);
    }
    today = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
    atDate = DATE_TIME_FORMAT.parse(today + " " + "23:59:57");
    trigger = make(schedule);
    assertNotNull(trigger);
    assertTrue(trigger instanceof SimpleTriggerImpl);
    assertEquals(atDate, trigger.getStartTime());
  }

  @Test
  public void testCronTrigger() {
    String schedule = "(0 0/5 14,18 * * ?)";
    Trigger trigger = make(schedule);
    assertNotNull(trigger);
    assertTrue(trigger instanceof CronTrigger);
    assertEquals("0 0/5 14,18 * * ?", ((CronTrigger) trigger).getCronExpression());
  }

  @Test
  public void testInvalidSchedule() {
    String schedule = "invalid schedule";
    assertThrows(IllegalArgumentException.class, () -> make(schedule));
  }
}
