package cn.edu.tsinghua.iginx.transform.pojo;

import cn.edu.tsinghua.iginx.utils.JsonUtils;
import lombok.Data;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.triggers.SimpleTriggerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.Date;
import java.util.Objects;

@Data
public class TriggerDescriptor {
  private static final Logger LOGGER = LoggerFactory.getLogger(TriggerDescriptor.class);

  public enum TriggerType {
    EVERY,
    AFTER,
    AT,
    CRON
  }

  private TriggerType type;
  // common
  private String name;
  private String group;
  private String description;
  private Date startDate;
  private Date endDate;

  // every
  private Long repeatInterval;

  // cron
  private String cronExpression;

  public static TriggerDescriptor toTriggerDescriptor(Trigger trigger) {
    TriggerDescriptor triggerDescriptor = getInitDescriptor(trigger);
    switch (TriggerDescriptor.TriggerType.valueOf(trigger.getDescription())) {
      case EVERY:
        return toEveryTriggerDescriptor(triggerDescriptor, (SimpleTriggerImpl) trigger);
      case AFTER:
      case AT:
        return toAfterAtTriggerDescriptor(triggerDescriptor, (SimpleTriggerImpl) trigger);
      case CRON:
        return toCronTriggerDescriptor(triggerDescriptor, (CronTrigger) trigger);
      default:
        LOGGER.error("Invalid trigger type:{}", trigger.getDescription());
        return null;
    }
  }

  private static TriggerDescriptor getInitDescriptor(Trigger trigger) {
    TriggerDescriptor descriptor = new TriggerDescriptor();
    descriptor.setName(trigger.getKey().getName());
    descriptor.setGroup(trigger.getKey().getGroup());
    descriptor.setStartDate(trigger.getStartTime());
    descriptor.setEndDate(trigger.getEndTime());
    descriptor.setDescription(trigger.getDescription());
    descriptor.setType(TriggerType.valueOf(trigger.getDescription()));
    return descriptor;
  }

  private static TriggerDescriptor toEveryTriggerDescriptor(TriggerDescriptor descriptor, SimpleTriggerImpl trigger) {
    descriptor.setRepeatInterval(trigger.getRepeatInterval());
    return descriptor;
  }

  private static TriggerDescriptor toAfterAtTriggerDescriptor(TriggerDescriptor descriptor, SimpleTriggerImpl trigger) {
    // start date is all we need
    return descriptor;
  }

  private static TriggerDescriptor toCronTriggerDescriptor(TriggerDescriptor descriptor, CronTrigger trigger) {
    descriptor.setCronExpression(trigger.getCronExpression());
    return descriptor;
  }

  public static Trigger fromTriggerDescriptor(TriggerDescriptor descriptor) {
    TriggerBuilder<Trigger> builder = TriggerBuilder.newTrigger()
            .withIdentity(descriptor.getName(), descriptor.getGroup())
            .withDescription(descriptor.getDescription());
    switch (descriptor.getType()) {
      case EVERY:
        return fromEveryTriggerDescriptor(builder, descriptor);
      case AFTER:
      case AT:
        return fromAfterAtTriggerDescriptor(builder, descriptor);
      case CRON:
        return fromCronTriggerDescriptor(builder, descriptor);
      default:
        LOGGER.error("Invalid descriptor type:{}", descriptor.getType());
        return null;
    }
  }

  private static Trigger fromEveryTriggerDescriptor(TriggerBuilder<Trigger> builder, TriggerDescriptor descriptor) {
    Calendar now = Calendar.getInstance();
    if (descriptor.endDate != null && now.getTime().after(descriptor.endDate)) {
      LOGGER.warn("trigger({}) is supposed to end at {} before current time.", descriptor.getName(), descriptor.getEndDate());
      return null;
    }
    Date start = descriptor.startDate;
    if (start == null) {
      LOGGER.error("trigger({}) should set start date.", descriptor.getName());
      return null;
    }
    long interval = descriptor.getRepeatInterval();
    if (now.getTime().after(start)) {
      // past executions
      long elapsedTime = now.getTime().getTime() - start.getTime();
      long missedExecutions = elapsedTime / interval;

      // next time should be:
      start = new Date(start.getTime() + (missedExecutions + 1) * interval);
    }
    return builder.startAt(start).endAt(descriptor.getEndDate()).withSchedule(
                    SimpleScheduleBuilder.simpleSchedule()
                            .withIntervalInMilliseconds(interval)
                            .withMisfireHandlingInstructionFireNow()
                            .repeatForever())
            .build();
  }

  private static Trigger fromAfterAtTriggerDescriptor(TriggerBuilder<Trigger> builder, TriggerDescriptor descriptor) {
    Calendar now = Calendar.getInstance();
    if (now.getTime().after(descriptor.startDate)) {
      LOGGER.warn("trigger({}) is supposed to start at {} before current time.", descriptor.getName(), descriptor.startDate);
      return null;
    }
    return builder.startAt(descriptor.startDate)
            .withSchedule(
                    SimpleScheduleBuilder.simpleSchedule()
                            .withMisfireHandlingInstructionFireNow()
                            .withRepeatCount(0))
            .build();
  }

  private static Trigger fromCronTriggerDescriptor(TriggerBuilder<Trigger> builder, TriggerDescriptor descriptor) {
    return builder.withSchedule(CronScheduleBuilder.cronSchedule(descriptor.getCronExpression())).build();
  }

  public TriggerDescriptor copy() {
    TriggerDescriptor copy = new TriggerDescriptor();
    copy.setRepeatInterval(this.repeatInterval);
    copy.setCronExpression(this.cronExpression);
    copy.setType(this.type);
    copy.setName(this.name);
    copy.setGroup(this.group);
    copy.setDescription(this.description);
    // ensure deep copy
    if (this.startDate != null) {
      copy.setStartDate(new Date(this.startDate.getTime()));
    }
    if (this.endDate != null) {
      copy.setEndDate(new Date(this.endDate.getTime()));
    }
    return copy;
  }

  public boolean equals(TriggerDescriptor that) {
    return that.type == this.type && that.name.equals(this.name) && that.group.equals(this.group)
            && that.description.equals(this.description)
            && Objects.equals(that.startDate, this.startDate)
            && Objects.equals(that.endDate, this.endDate)
            && Objects.equals(that.repeatInterval, this.repeatInterval)
            && Objects.equals(that.cronExpression, this.cronExpression);
  }

}
