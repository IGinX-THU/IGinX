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
package cn.edu.tsinghua.iginx.transform.pojo;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.validation.constraints.NotNull;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobScheduleTriggerMaker {

  private static final Logger LOGGER = LoggerFactory.getLogger(JobScheduleTriggerMaker.class);
  private static final SimpleDateFormat DATE_TIME_FORMAT =
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  private static final SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("HH:mm:ss");
  private static final String weekdayRegex =
      "(mon|tue|wed|thu|fri|sat|sun|monday|tuesday|wednesday|thursday|friday|saturday|sunday)";

  private static final Pattern everyPattern =
      Pattern.compile(
          "(?i)^every\\s+(\\d+)\\s+(second|minute|hour|day|month|year)(?:\\s+starts\\s+'([^']+)')?(?:\\s+ends\\s+'([^']+)')?$");

  private static final Pattern everyWeekdayPattern =
      Pattern.compile(String.format("(?i)^every ((?:%s,?)+)$", weekdayRegex));

  private static final Pattern afterPattern =
      Pattern.compile("(?i)^after\\s+(\\d+)\\s+(second|minute|hour|day|month|year)$");

  private static final Pattern atPattern =
      Pattern.compile("(?i)^at\\s+'((?:\\d{4}-\\d{2}-\\d{2}\\s+)?\\d{2}:\\d{2}:\\d{2})'$");

  private static String errMsg;

  private static Date now;

  private enum INTERVAL_ENUM {
    SECOND,
    MINUTE,
    HOUR,
    DAY,
    MONTH,
    YEAR;

    public static INTERVAL_ENUM matcher(String unit) {
      unit = unit.trim().toLowerCase();
      switch (unit) {
        case "second":
          return SECOND;
        case "minute":
          return MINUTE;
        case "hour":
          return HOUR;
        case "day":
          return DAY;
        case "month":
          return MONTH;
        case "year":
          return YEAR;
        default:
          return null;
      }
    }
  }

  /**
   * 根据调度字符串生成 Quartz Trigger。调度字符串大致可分为以下四种： 1. every 3 second/minute/hour/day/month/year
   * 每隔3秒/分/小时/天/月/年执行一次，可以添加开始时间和结束时间，两个时间必须用单引号包围，例如： every 10 minute starts '2024-02-03 12:00:00'
   * ends '2024-02-04 12:00:00' 2. after 3 second/minute/hour/day/month/year 在3秒/分/小时/天/月/年后执行一次; 3.
   * at (yyyy-MM-dd)? HH:mm:ss 在指定时间执行 4. (* * * * * *) cron格式的字符串
   *
   * @param jobSchedule 调度字符串，控制任务执行的时间
   * @return 返回在指定时间触发的Quartz触发器
   */
  public static Trigger getTrigger(@NotNull String jobSchedule) {
    // corn in quartz is not case-sensitive
    jobSchedule = jobSchedule.trim().toLowerCase();
    now = new Date();
    if (jobSchedule.isEmpty()) {
      throw new IllegalArgumentException("Job schedule indicator string is empty.");
    }
    if (jobSchedule.startsWith("every")) {
      if (jobSchedule.matches(String.valueOf(everyWeekdayPattern))) {
        return everyWeeklyTrigger(jobSchedule);
      }
      return everyTrigger(jobSchedule);
    } else if (jobSchedule.startsWith("after")) {
      return afterTrigger(jobSchedule);
    } else if (jobSchedule.startsWith("at")) {
      return atTrigger(jobSchedule);
    } else if (jobSchedule.matches("\\(.*\\)")) {
      return cronTrigger(jobSchedule);
    }

    throw new IllegalArgumentException("Invalid time format: " + jobSchedule);
  }

  private static Trigger everyTrigger(String jobSchedule) {
    Matcher nomalEverymatcher = everyPattern.matcher(jobSchedule);
    TriggerBuilder<Trigger> triggerBuilder = TriggerBuilder.newTrigger();

    if (nomalEverymatcher.matches()) {
      int intervalValue = Integer.parseInt(nomalEverymatcher.group(1));
      INTERVAL_ENUM intervalUnit = INTERVAL_ENUM.matcher(nomalEverymatcher.group(2));
      if (intervalUnit == null) {
        LOGGER.error(
            "Error parsing interval unit {}. Available: second, minute, hour, day.",
            nomalEverymatcher.group(2));
        throw new IllegalArgumentException(
            "Error parsing interval unit "
                + nomalEverymatcher.group(2)
                + ". Available: second, minute, hour, day.");
      }
      String starts = nomalEverymatcher.group(3);
      String ends = nomalEverymatcher.group(4);

      switch (intervalUnit) {
        case SECOND:
          triggerBuilder.withSchedule(
              SimpleScheduleBuilder.simpleSchedule()
                  .withIntervalInSeconds(intervalValue)
                  .repeatForever());
          break;
        case MINUTE:
          triggerBuilder.withSchedule(
              SimpleScheduleBuilder.simpleSchedule()
                  .withIntervalInMinutes(intervalValue)
                  .repeatForever());
          break;
        case HOUR:
          triggerBuilder.withSchedule(
              SimpleScheduleBuilder.simpleSchedule()
                  .withIntervalInHours(intervalValue)
                  .repeatForever());
          break;
        case DAY:
          triggerBuilder.withSchedule(
              SimpleScheduleBuilder.simpleSchedule()
                  .withIntervalInHours(intervalValue * 24)
                  .repeatForever());
          break;
        case MONTH:
          triggerBuilder.withSchedule(
              CalendarIntervalScheduleBuilder.calendarIntervalSchedule()
                  .withIntervalInMonths(intervalValue));
          break;
        case YEAR:
          triggerBuilder.withSchedule(
              CalendarIntervalScheduleBuilder.calendarIntervalSchedule()
                  .withIntervalInYears(intervalValue));
          break;
      }

      Date startDate = null, endDate = null;

      if (starts != null) {
        try {
          startDate = parseDate(starts);
          if (startDate.before(now)) {
            errMsg = String.format("Start Time %s is before current time.", starts);
            LOGGER.error(errMsg);
            throw new IllegalArgumentException(errMsg);
          }
          triggerBuilder.startAt(startDate);
        } catch (ParseException e) {
          errMsg = String.format("Error parsing start time %s. Please refer to manual.", starts);
          LOGGER.error(errMsg, e);
          throw new IllegalArgumentException(errMsg);
        }
      } else {
        triggerBuilder.startAt(now);
      }

      if (ends != null) {
        try {
          endDate = parseDate(ends);
          if (startDate != null && endDate.before(startDate)) {
            errMsg = String.format("End Time %s is before start time %s.", ends, starts);
            LOGGER.error(errMsg);
            throw new IllegalArgumentException(errMsg);
          }
          if (endDate.before(now)) {
            errMsg = String.format("End Time %s is before current time.", ends);
            LOGGER.error(errMsg);
            throw new IllegalArgumentException(errMsg);
          }
          triggerBuilder.endAt(endDate);
        } catch (ParseException e) {
          errMsg = String.format("Error parsing end time %s. Please refer to manual.", ends);
          LOGGER.error(errMsg);
          throw new IllegalArgumentException(errMsg);
        }
      }
    } else {
      errMsg =
          String.format("Error parsing schedule string %s. Please refer to manual.", jobSchedule);
      LOGGER.error(errMsg);
      throw new IllegalArgumentException(errMsg);
    }
    return triggerBuilder.build();
  }

  private static Trigger everyWeeklyTrigger(String jobSchedule) {
    Matcher matcher = everyWeekdayPattern.matcher(jobSchedule);
    if (matcher.matches()) {
      String daysString = matcher.group(1);
      String[] daysArray = daysString.split(",");
      Set<Integer> daysOfWeek = new HashSet<>();

      for (String day : daysArray) {
        day = day.trim().toLowerCase(Locale.ROOT);
        switch (day) {
          case "mon":
          case "monday":
            daysOfWeek.add(DateBuilder.MONDAY);
            break;
          case "tue":
          case "tuesday":
            daysOfWeek.add(DateBuilder.TUESDAY);
            break;
          case "wed":
          case "wednesday":
            daysOfWeek.add(DateBuilder.WEDNESDAY);
            break;
          case "thu":
          case "thursday":
            daysOfWeek.add(DateBuilder.THURSDAY);
            break;
          case "fri":
          case "friday":
            daysOfWeek.add(DateBuilder.FRIDAY);
            break;
          case "sat":
          case "saturday":
            daysOfWeek.add(DateBuilder.SATURDAY);
            break;
          case "sun":
          case "sunday":
            daysOfWeek.add(DateBuilder.SUNDAY);
            break;
          default:
            throw new IllegalArgumentException("Invalid day of the week: " + day);
        }
      }
      return TriggerBuilder.newTrigger()
          .withSchedule(
              DailyTimeIntervalScheduleBuilder.dailyTimeIntervalSchedule()
                  .onDaysOfTheWeek(daysOfWeek.toArray(new Integer[0])))
          .build();
    }
    throw new IllegalArgumentException("Invalid weekly format: " + jobSchedule);
  }

  private static Trigger afterTrigger(String jobSchedule) {
    Matcher afterMatcher = afterPattern.matcher(jobSchedule);
    TriggerBuilder<Trigger> triggerBuilder = TriggerBuilder.newTrigger();

    if (afterMatcher.matches()) {
      int intervalValue = Integer.parseInt(afterMatcher.group(1));
      INTERVAL_ENUM intervalUnit = INTERVAL_ENUM.matcher(afterMatcher.group(2));
      if (intervalUnit == null) {
        LOGGER.error(
            "Error parsing interval unit {}. Available: second, minute, hour, day.",
            afterMatcher.group(2));
        throw new IllegalArgumentException(
            "Error parsing interval unit "
                + afterMatcher.group(2)
                + ". Available: second, minute, hour, day.");
      }

      switch (intervalUnit) {
        case SECOND:
          triggerBuilder.startAt(
              DateBuilder.futureDate(intervalValue, DateBuilder.IntervalUnit.SECOND));
          break;
        case MINUTE:
          triggerBuilder.startAt(
              DateBuilder.futureDate(intervalValue, DateBuilder.IntervalUnit.MINUTE));
          break;
        case HOUR:
          triggerBuilder.startAt(
              DateBuilder.futureDate(intervalValue, DateBuilder.IntervalUnit.HOUR));
          break;
        case DAY:
          triggerBuilder.startAt(
              DateBuilder.futureDate(intervalValue, DateBuilder.IntervalUnit.DAY));
          break;
        case MONTH:
          triggerBuilder.startAt(
              DateBuilder.futureDate(intervalValue, DateBuilder.IntervalUnit.MONTH));
          break;
        case YEAR:
          triggerBuilder.startAt(
              DateBuilder.futureDate(intervalValue, DateBuilder.IntervalUnit.YEAR));
          break;
      }
    } else {
      LOGGER.error("Error parsing schedule string {}. Please refer to manual.", jobSchedule);
      throw new IllegalArgumentException(
          "Error parsing schedule string " + jobSchedule + ". Please refer to manual.");
    }
    return triggerBuilder.build();
  }

  private static Trigger atTrigger(String jobSchedule) {
    Matcher atMatcher = atPattern.matcher(jobSchedule);

    if (atMatcher.matches()) {
      String atTime = atMatcher.group(1);
      try {
        Date atDate = parseDate(atTime);
        if (atDate.before(now)) {
          errMsg =
              String.format("Trying to trigger a job at %s which is before current time", atTime);
          LOGGER.error(errMsg);
          throw new IllegalArgumentException(errMsg);
        }
        return TriggerBuilder.newTrigger()
            .startAt(atDate)
            .withSchedule(
                SimpleScheduleBuilder.simpleSchedule()
                    .withMisfireHandlingInstructionFireNow()
                    .withRepeatCount(0))
            .build();
      } catch (ParseException e) {
        errMsg = String.format("Error parsing time %s. Please refer to manual.", atTime);
        LOGGER.error(errMsg);
        throw new IllegalArgumentException(errMsg);
      }
    } else {
      LOGGER.error("Error parsing schedule string {}. Please refer to manual.", jobSchedule);
      throw new IllegalArgumentException(
          "Error parsing schedule string " + jobSchedule + ". Please refer to manual.");
    }
  }

  private static Trigger cronTrigger(String jobSchedule) {
    String cronExpression = jobSchedule.substring(1, jobSchedule.length() - 1);
    if (cronExpression.isEmpty()) {
      errMsg = "Cron string is empty. Please provide a valid corn expression.";
      LOGGER.error(errMsg);
      throw new IllegalArgumentException(errMsg);
    }
    if (!CronExpression.isValidExpression(cronExpression)) {
      errMsg =
          String.format(
              "Cron string (%s) is not valid. Please provide a valid cron expression.",
              cronExpression);
      LOGGER.error(errMsg);
      throw new IllegalArgumentException(errMsg);
    }
    return TriggerBuilder.newTrigger()
        .withSchedule(CronScheduleBuilder.cronSchedule(cronExpression))
        .build();
  }

  /**
   * 解析日期“HH:mm:ss”格式或者"yyyy-MM-dd HH:mm:ss"格式
   *
   * @param dateString 日期字符串
   * @return 解析得到的Date类型变量
   * @throws ParseException
   */
  private static Date parseDate(String dateString) throws ParseException {
    if (dateString == null || dateString.isEmpty()) {
      return null;
    }
    if (dateString.length() == 8) { // 'HH:mm:ss' format
      // Set the date part to the current date
      String today = new SimpleDateFormat("yyyy-MM-dd").format(now);
      dateString = today + " " + dateString;
    }
    // 'yyyy-MM-dd HH:mm:ss' format
    return DATE_TIME_FORMAT.parse(dateString);
  }
}
