package cn.edu.tsinghua.iginx.transform.pojo;

import java.time.LocalTime;
import java.util.Date;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.validation.constraints.NotNull;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobScheduleTrigger {

  private static final Logger LOGGER = LoggerFactory.getLogger(JobScheduleTrigger.class);
  public static final String weekdayRegex =
      "(mon|tue|wed|thu|fri|sat|sun|monday|tuesday|wednesday|thursday|friday|saturday|sunday)";

  private enum INTERVAL_ENUM {
    SECOND,
    MINUTE,
    HOUR,
    DAY
  }

  /**
   * 根据调度格式生成 Quartz Trigger。调度控制用字符串定义，大致可分为以下四种： 1. every 3s/m/h/d 每隔3秒/分/小时/天执行一次； every mon,wed
   * 每个周一、周三执行一次； every 16:00:00 每天16点执行一次 2. after 3s/m/h/d 在3秒/分/小时/天后执行一次 3. (* * * * *)
   * cron格式的字符串
   *
   * @param jobSchedule 字符串，控制任务执行的时间
   * @return 返回在指定时间出发的Quartz触发器
   */
  public static Trigger getTrigger(@NotNull String jobSchedule) {
    jobSchedule = jobSchedule.trim().toLowerCase();
    if (jobSchedule.isEmpty()) {
      throw new IllegalArgumentException("Job schedule indicator string is empty.");
    }
    if (jobSchedule.startsWith("every")) {
      if (jobSchedule.matches("every \\d+s")) {
        return createEverySecondTrigger(jobSchedule);
      } else if (jobSchedule.matches("every \\d+m")) {
        return createEveryMinuteTrigger(jobSchedule);
      } else if (jobSchedule.matches("every \\d+h")) {
        return createEveryHourTrigger(jobSchedule);
      } else if (jobSchedule.matches("every \\d+d")) {
        return createEveryDayTrigger(jobSchedule);
      } else if (jobSchedule.matches("every \\d{2}:\\d{2}:\\d{2}")) {
        return createDailyTimeTrigger(jobSchedule);
      } else if (jobSchedule.matches(String.format("(?i)every (%s,?)+", weekdayRegex))) {
        return createWeeklyTrigger(jobSchedule);
      }
    } else if (jobSchedule.startsWith("after")) {
      if (jobSchedule.matches("after \\d+s")) {
        return createAfterSecondsTrigger(jobSchedule);
      } else if (jobSchedule.matches("after \\d+m")) {
        return createAfterMinutesTrigger(jobSchedule);
      } else if (jobSchedule.matches("after \\d+h")) {
        return createAfterHoursTrigger(jobSchedule);
      } else if (jobSchedule.matches("ater \\d+d")) {
        return createAfterDaysTrigger(jobSchedule);
      }
    } else if (jobSchedule.matches("at \\d{2}:\\d{2}:\\d{2}")) {
      return createAtTimeTrigger(jobSchedule);
    } else if (jobSchedule.matches("\\(.*\\)")) {
      return createCronTrigger(jobSchedule);
    }

    throw new IllegalArgumentException("Invalid time format: " + jobSchedule);
  }

  private static Trigger createEverySecondTrigger(String jobSchedule) {
    int seconds = Integer.parseInt(jobSchedule.split(" ")[1].replace("s", ""));
    return TriggerBuilder.newTrigger()
        .startNow()
        .withSchedule(
            SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(seconds).repeatForever())
        .build();
  }

  private static Trigger createEveryMinuteTrigger(String jobSchedule) {
    int minutes = Integer.parseInt(jobSchedule.split(" ")[1].replace("m", ""));
    return TriggerBuilder.newTrigger()
        .startNow()
        .withSchedule(
            SimpleScheduleBuilder.simpleSchedule().withIntervalInMinutes(minutes).repeatForever())
        .build();
  }

  private static Trigger createEveryHourTrigger(String jobSchedule) {
    int hours = Integer.parseInt(jobSchedule.split(" ")[1].replace("h", ""));
    return TriggerBuilder.newTrigger()
        .startNow()
        .withSchedule(
            SimpleScheduleBuilder.simpleSchedule().withIntervalInHours(hours).repeatForever())
        .build();
  }

  private static Trigger createEveryDayTrigger(String jobSchedule) {
    int days = Integer.parseInt(jobSchedule.split(" ")[1].replace("d", ""));
    return TriggerBuilder.newTrigger()
        .startNow()
        .withSchedule(
            SimpleScheduleBuilder.simpleSchedule().withIntervalInHours(days * 24).repeatForever())
        .build();
  }

  private static Trigger createDailyTimeTrigger(String jobSchedule) {
    String time = jobSchedule.split(" ")[1];
    LocalTime localTime = LocalTime.parse(time);
    return TriggerBuilder.newTrigger()
        .startNow()
        .withSchedule(
            DailyTimeIntervalScheduleBuilder.dailyTimeIntervalSchedule()
                .startingDailyAt(
                    TimeOfDay.hourAndMinuteOfDay(localTime.getHour(), localTime.getMinute()))
                .withIntervalInHours(24)
                .onEveryDay())
        .build();
  }

  private static Trigger createWeeklyTrigger(String jobSchedule) {
    Pattern pattern = Pattern.compile(String.format("(?i)every (%s,?)+", weekdayRegex));
    Matcher matcher = pattern.matcher(jobSchedule);
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

  private static Trigger createAfterSecondsTrigger(String jobSchedule) {
    int seconds = Integer.parseInt(jobSchedule.split(" ")[1].replace("s", "").trim());
    return TriggerBuilder.newTrigger()
        .startAt(DateBuilder.futureDate(seconds, DateBuilder.IntervalUnit.SECOND))
        .build();
  }

  private static Trigger createAfterMinutesTrigger(String jobSchedule) {
    int minutes = Integer.parseInt(jobSchedule.split(" ")[1].replace("m", "").trim());
    return TriggerBuilder.newTrigger()
        .startAt(DateBuilder.futureDate(minutes, DateBuilder.IntervalUnit.MINUTE))
        .build();
  }

  private static Trigger createAfterHoursTrigger(String jobSchedule) {
    int hours = Integer.parseInt(jobSchedule.split(" ")[1].replace("h", ""));
    return TriggerBuilder.newTrigger()
        .startAt(DateBuilder.futureDate(hours, DateBuilder.IntervalUnit.HOUR))
        .build();
  }

  private static Trigger createAfterDaysTrigger(String jobSchedule) {
    int days = Integer.parseInt(jobSchedule.split(" ")[1].replace("d", ""));
    return TriggerBuilder.newTrigger()
        .startAt(DateBuilder.futureDate(days, DateBuilder.IntervalUnit.DAY))
        .build();
  }

  private static Trigger createAtTimeTrigger(String jobSchedule) {
    String time = jobSchedule.split(" ")[1];
    LocalTime localTime = LocalTime.parse(time);
    Date startTime =
        DateBuilder.todayAt(localTime.getHour(), localTime.getMinute(), localTime.getSecond());

    if (startTime.before(new Date())) {
      startTime =
          DateBuilder.tomorrowAt(localTime.getHour(), localTime.getMinute(), localTime.getSecond());
    }

    return TriggerBuilder.newTrigger()
        .startAt(startTime)
        .withSchedule(
            SimpleScheduleBuilder.simpleSchedule()
                .withMisfireHandlingInstructionFireNow()
                .withRepeatCount(0))
        .build();
  }

  private static Trigger createCronTrigger(String jobSchedule) {
    String cronExpression = jobSchedule.substring(1, jobSchedule.length() - 1);
    return TriggerBuilder.newTrigger()
        .withSchedule(CronScheduleBuilder.cronSchedule(cronExpression))
        .build();
  }
}
