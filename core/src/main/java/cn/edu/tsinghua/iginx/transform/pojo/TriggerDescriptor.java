package cn.edu.tsinghua.iginx.transform.pojo;

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

@Data
public class TriggerDescriptor {
  private static final Logger LOGGER = LoggerFactory.getLogger(TriggerDescriptor.class);

  public enum TriggerType {
    EVERY,
    AFTER,
    AT,
    CRON
  }

  public TriggerType type;
  // common
  protected String name;
  protected String group;
  protected String description;
  protected Date startDate;
  protected Date endDate;

  // every-weekly has been converted to cron in TriggerMaker
  @Data
  public static class EveryTriggerDescriptor extends TriggerDescriptor {
    public TriggerType type = TriggerType.EVERY;

    private Long repeatInterval;
  }

  @Data
  public static class AfterAtTriggerDescriptor extends TriggerDescriptor {
    public TriggerType type = TriggerType.AFTER;
  }

  @Data
  public static class CronTriggerDescriptor extends TriggerDescriptor {
    public TriggerType type = TriggerType.CRON;

    private String cronExpression;
  }



  public static TriggerDescriptor toTriggerDescriptor(Trigger trigger) {
    TriggerDescriptor triggerDescriptor = getInitDescriptor(trigger);
    switch (TriggerDescriptor.TriggerType.valueOf(trigger.getDescription())) {
      case EVERY:
        return toEveryTriggerDescriptor((TriggerDescriptor.EveryTriggerDescriptor) triggerDescriptor, (SimpleTriggerImpl) trigger);
      case AFTER:
      case AT:
        return toAfterAtTriggerDescriptor((TriggerDescriptor.AfterAtTriggerDescriptor) triggerDescriptor, (SimpleTriggerImpl) trigger);
      case CRON:
        return toCronTriggerDescriptor((TriggerDescriptor.CronTriggerDescriptor) triggerDescriptor, (CronTrigger) trigger);
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
    return descriptor;
  }

  private static EveryTriggerDescriptor toEveryTriggerDescriptor(EveryTriggerDescriptor descriptor, SimpleTriggerImpl trigger) {
    descriptor.setRepeatInterval(trigger.getRepeatInterval());
    return descriptor;
  }

  private static TriggerDescriptor toAfterAtTriggerDescriptor(AfterAtTriggerDescriptor descriptor, SimpleTriggerImpl trigger) {
    // start date is all we need
    return descriptor;
  }

  private static CronTriggerDescriptor toCronTriggerDescriptor(CronTriggerDescriptor descriptor, CronTrigger trigger) {
    descriptor.setCronExpression(trigger.getCronExpression());
    return descriptor;
  }

  public static Trigger fromTriggerDescriptor(TriggerDescriptor descriptor) {
    TriggerBuilder<Trigger> builder = TriggerBuilder.newTrigger()
            .withIdentity(descriptor.getName(), descriptor.getGroup());
    builder.startAt(descriptor.startDate);
    builder.endAt(descriptor.getEndDate());
    switch (descriptor.getType()) {
      case EVERY:
        return fromEveryTriggerDescriptor(builder, (EveryTriggerDescriptor) descriptor);
      case AFTER:
      case AT:
        return fromAfterAtTriggerDescriptor(builder, descriptor);
      case CRON:
        return fromCronTriggerDescriptor(builder, (CronTriggerDescriptor) descriptor);
      default:
        LOGGER.error("Invalid descriptor type:{}", descriptor.getType());
        return null;
    }
  }

  private static Trigger fromEveryTriggerDescriptor(TriggerBuilder<Trigger> builder, EveryTriggerDescriptor descriptor) {
    Calendar now = Calendar.getInstance();
    if (descriptor.endDate != null && now.before(descriptor.endDate)) {
      LOGGER.warn("trigger({}) is supposed to end at {} before current time.", descriptor.getName(), descriptor.getEndDate());
      return null;
    }
    Date start = descriptor.startDate;
    long interval = descriptor.getRepeatInterval();
    if (now.after(start)) {
      // past executions
      long elapsedTime = now.getTime().getTime() - start.getTime();
      long missedExecutions = elapsedTime / interval;

      // next time should be:
      start = new Date(start.getTime() + (missedExecutions + 1) * interval);
    }
    return builder.startAt(start).withSchedule(
                    SimpleScheduleBuilder.simpleSchedule()
                            .withIntervalInMilliseconds(interval)
                            .withMisfireHandlingInstructionFireNow()
                            .repeatForever())
            .build();
  }

  private static Trigger fromAfterAtTriggerDescriptor(TriggerBuilder<Trigger> builder, TriggerDescriptor descriptor) {
    Calendar now = Calendar.getInstance();
    if (now.before(descriptor.startDate)) {
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

  private static Trigger fromCronTriggerDescriptor(TriggerBuilder<Trigger> builder, CronTriggerDescriptor descriptor) {
    return builder.withSchedule(CronScheduleBuilder.cronSchedule(descriptor.getCronExpression())).build();
  }

  public TriggerDescriptor copy() {
    TriggerDescriptor copy;
    if (this instanceof EveryTriggerDescriptor) {
      EveryTriggerDescriptor newCopy = new EveryTriggerDescriptor();
      newCopy.setRepeatInterval(((EveryTriggerDescriptor) this).getRepeatInterval());
      copy = newCopy;
    }
    else if (this instanceof AfterAtTriggerDescriptor) {
      copy = new AfterAtTriggerDescriptor();
    }
    else if (this instanceof CronTriggerDescriptor) {
      CronTriggerDescriptor newCopy = new CronTriggerDescriptor();
      newCopy.setCronExpression(((CronTriggerDescriptor) this).getCronExpression());
      copy = newCopy;
    }
    else {
      LOGGER.warn("Unknown TriggerDescriptor type: {}", this.getClass().getName());
      copy = new TriggerDescriptor();
    }
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
}
