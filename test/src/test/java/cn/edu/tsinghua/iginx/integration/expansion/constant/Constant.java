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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.integration.expansion.constant;

import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Constant {
  public static final String ORI_PORT_NAME = "ori_port";
  public static final String EXP_PORT_NAME = "exp_port";
  public static final String READ_ONLY_PORT_NAME = "read_only_port";

  // port
  public static int oriPort = 6667;

  public static int expPort = 6668;

  public static int readOnlyPort = 6669;

  public static int oriPortIginx = 6888;

  public static int expPortIginx = 6889;

  public static int readOnlyPortIginx = 6890;

  // path
  public static final List<String> INIT_PATH_LIST =
      Arrays.asList(Controller.DUMMY_INIT_PATH_BEGIN, Controller.DUMMY_INIT_PATH_END);

  public static final List<String> ORI_PATH_LIST =
      Arrays.asList("mn.wf01.wt01.status", "mn.wf01.wt01.temperature");

  public static final List<String> ORI_EXTEND_PATH_LIST =
      Arrays.asList("a.a.a.status", "a.a.a.temperature");

  public static final List<String> EXP_PATH_LIST =
      Arrays.asList("nt.wf03.wt01.status2", "nt.wf04.wt01.temperature");

  public static final List<String> EXP_EXTEND_PATH_LIST =
      Arrays.asList("a.a.b.status2", "a.a.b.temperature");

  public static final List<String> EXP_PATH_LIST1 =
      Collections.singletonList("nt.wf03.wt01.status2");

  public static final List<String> EXP_PATH_LIST2 =
      Collections.singletonList("nt.wf04.wt01.temperature");

  public static final List<String> READ_ONLY_PATH_LIST =
      Arrays.asList("tm.wf05.wt01.status", "tm.wf05.wt01.temperature");

  public static final List<String> READ_ONLY_EXTEND_PATH_LIST =
      Arrays.asList("a.a.c.status", "a.a.c.temperature");

  // data type
  public static List<DataType> INIT_DATA_TYPE_LIST = Arrays.asList(DataType.LONG, DataType.LONG);

  public static List<DataType> ORI_DATA_TYPE_LIST = Arrays.asList(DataType.LONG, DataType.DOUBLE);

  public static List<DataType> EXP_DATA_TYPE_LIST = Arrays.asList(DataType.LONG, DataType.DOUBLE);

  public static List<DataType> READ_ONLY_DATA_TYPE_LIST =
      Arrays.asList(DataType.LONG, DataType.DOUBLE);

  // values
  public static List<List<Object>> INIT_VALUES_LIST =
      Arrays.asList(Arrays.asList(11111111L, 11111111L), Arrays.asList(22222222L, 22222222L));

  public static List<List<Object>> ORI_VALUES_LIST =
      Arrays.asList(Arrays.asList(11111111L, 15123.27), Arrays.asList(22222222L, 20123.71));

  public static List<List<Object>> ORI_EXTEND_VALUES_LIST =
      Collections.singletonList(Arrays.asList(123456L, 654321.2));

  public static List<List<Object>> EXP_VALUES_LIST =
      Arrays.asList(Arrays.asList(33333333L, 66123.23), Arrays.asList(44444444L, 77123.71));

  public static List<List<Object>> EXP_EXTEND_VALUES_LIST =
      Collections.singletonList(Arrays.asList(77777777L, 3498.1));

  public static List<List<Object>> EXP_EXTEND_VALUES_LIST1 =
      Collections.singletonList(Collections.singletonList(77777777L));

  public static List<List<Object>> EXP_EXTEND_VALUES_LIST2 =
      Collections.singletonList(Collections.singletonList(3498.1));

  public static List<List<Object>> EXP_VALUES_LIST1 =
      Arrays.asList(Collections.singletonList(33333333L), Collections.singletonList(44444444L));

  public static List<List<Object>> EXP_VALUES_LIST2 =
      Arrays.asList(Collections.singletonList(66123.23), Collections.singletonList(77123.71));

  public static List<List<Object>> READ_ONLY_VALUES_LIST =
      Arrays.asList(Arrays.asList(55555555L, 10012.01), Arrays.asList(66666666L, 99123.99));

  public static List<List<Object>> READ_ONLY_EXTEND_VALUES_LIST =
      Collections.singletonList(Arrays.asList(9999999L, 152346.1));

  public static List<List<Object>> REPEAT_EXP_VALUES_LIST1 =
      Arrays.asList(Arrays.asList(33333333L, 33333333L), Arrays.asList(44444444L, 44444444L));

  // key list
  public static List<Long> INIT_KEYS_LIST = Arrays.asList(1L, (long) Integer.MAX_VALUE);

  // for file system
  // for file system & influxdb
  public static Map<Integer, String> PORT_TO_ROOT =
      new HashMap<Integer, String>() {
        {
          put(oriPort, "mn");
          put(expPort, "nt");
          put(readOnlyPort, "tm");
        }
      };

  // for multi iginx test
  public static final Map<Integer, Integer> PORT_TO_RESTPORT =
      new HashMap<Integer, Integer>() {
        {
          put(oriPort, 6666);
          put(expPort, 6665);
          put(readOnlyPort, 6664);
        }
      };

  // for multi iginx test
  public static final Map<Integer, Integer> PORT_TO_IGINXPORT =
      new HashMap<Integer, Integer>() {
        {
          put(oriPort, oriPortIginx);
          put(expPort, expPortIginx);
          put(readOnlyPort, readOnlyPortIginx);
        }
      };

  // for parquet
  // <port, [dataDir, dataFilename]>
  private static final String oriDir = "mn";

  private static final String expDir = "nt";

  private static final String readOnlyDir = "tm";

  private static final String oriFilename = "oriData.parquet";

  private static final String expFilename = "expData.parquet";

  private static final String readOnlyFilename = "readOnlyData.parquet";

  public static final Map<Integer, List<String>> PARQUET_PARAMS =
      new HashMap<Integer, List<String>>() {
        {
          put(oriPort, Arrays.asList(oriDir, oriFilename));
          put(expPort, Arrays.asList(expDir, expFilename));
          put(readOnlyPort, Arrays.asList(readOnlyDir, readOnlyFilename));
        }
      };
}
