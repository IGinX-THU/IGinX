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
package cn.edu.tsinghua.iginx.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnowFlakeUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(SnowFlakeUtils.class);

  // 起始的时间戳
  private static final long START_STAMP = 0L;
  // 每一部分占用的位数，就三个
  private static final long SEQUENCE_BIT = 12; // 序列号占用的位数
  private static final long MACHINE_BIT = 9; // 机器标识占用的位数
  private static final long DATACENTER_BIT = 1; // 数据中心占用的位数
  // 每一部分最大值
  private static final long MAX_DATACENTER_NUM = ~(-1L << DATACENTER_BIT);
  private static final long MAX_MACHINE_NUM = ~(-1L << MACHINE_BIT);
  private static final long MAX_SEQUENCE = ~(-1L << SEQUENCE_BIT);
  // 每一部分向左的位移
  private static final long MACHINE_LEFT = SEQUENCE_BIT;
  private static final long DATACENTER_LEFT = SEQUENCE_BIT + MACHINE_BIT;
  private static final long TIMESTAMP_LEFT = DATACENTER_LEFT + DATACENTER_BIT;
  private static SnowFlakeUtils instance;
  private final long datacenterId; // 数据中心

  private final long machineId; // 机器标识

  private long sequence = 0L; // 序列号

  private long lastStamp = -1L; // 上一次时间戳

  public SnowFlakeUtils(long datacenterId, long machineId) {
    LOGGER.info("data center id: {}, machine id: {}", datacenterId, machineId);
    if (datacenterId > MAX_DATACENTER_NUM || datacenterId < 0) {
      throw new IllegalArgumentException(
          "datacenterId can't be greater than MAX_DATACENTER_NUM or less than 0");
    }
    if (machineId > MAX_MACHINE_NUM || machineId < 0) {
      throw new IllegalArgumentException(
          "machineId can't be greater than MAX_MACHINE_NUM or less than 0");
    }
    this.datacenterId = datacenterId;
    this.machineId = machineId;
  }

  public static void init(long machineId) {
    if (instance != null) {
      return;
    }
    instance = new SnowFlakeUtils(0, machineId);
  }

  public static SnowFlakeUtils getInstance() {
    return instance;
  }

  // 产生下一个ID
  public synchronized long nextId() {
    long currStamp = getNewTimestamp();
    // 如果当前时间小于上一次ID生成的时间戳，说明系统时钟回退过  这个时候应当抛出异常
    if (currStamp < lastStamp) {
      LOGGER.error("Clock moved backwards. Refusing to generate id");
      return 0L;
    }

    if (currStamp == lastStamp) {
      // if条件里表示当前调用和上一次调用落在了相同毫秒内，只能通过第三部分，序列号自增来判断为唯一，所以+1.
      sequence = (sequence + 1) & MAX_SEQUENCE;
      // 同一毫秒的序列数已经达到最大，只能等待下一个毫秒
      if (sequence == 0L) {
        currStamp = getNextMill();
      }
    }
    // 时间戳改变，毫秒内序列重置
    else {
      // 不同毫秒内，序列号置为0
      // 执行到这个分支的前提是currTimestamp > lastTimestamp，说明本次调用跟上次调用对比，已经不再同一个毫秒内了，这个时候序号可以重新回置0了。
      sequence = 0L;
    }

    lastStamp = currStamp;
    // 就是用相对毫秒数、机器ID和自增序号拼接
    // 移位  并通过  或运算拼到一起组成64位的ID
    return (currStamp - START_STAMP) << TIMESTAMP_LEFT // 时间戳部分
        | datacenterId << DATACENTER_LEFT // 数据中心部分
        | machineId << MACHINE_LEFT // 机器标识部分
        | sequence; // 序列号部分
  }

  private long getNextMill() {
    long mill = getNewTimestamp();
    // 使用while循环等待直到下一毫秒。
    while (mill <= lastStamp) {
      mill = getNewTimestamp();
    }
    return mill;
  }

  private long getNewTimestamp() {
    return System.currentTimeMillis();
  }
}
