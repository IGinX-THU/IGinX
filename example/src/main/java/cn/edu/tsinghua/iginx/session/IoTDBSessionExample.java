/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package cn.edu.tsinghua.iginx.session;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import cn.edu.tsinghua.iginx.thrift.DataType;
import org.apache.thrift.transport.TTransportException;

import java.util.ArrayList;
import java.util.List;

public class IoTDBSessionExample {

	private static Session session;

	private static final String S1 = "sg.d1.s1";
	private static final String S2 = "sg.d2.s2";
	private static final String S3 = "sg.d3.s3";
	private static final String S4 = "sg.d4.s4";

	public static void main(String[] args) throws SessionException, ExecutionException, TTransportException {
		session = new Session("127.0.0.1", 6324, "root", "root");
		session.openSession();

		insertRecords1();
//		insertRecords2();
		queryData();
//		aggregateQuery();
//		deleteDataInColumns();
//		queryData();

		session.closeSession();
	}

	private static void insertRecords1() throws SessionException, ExecutionException {
		List<String> paths = new ArrayList<>();
		paths.add(S2);

		long[] timestamps = new long[100];
		for (long i = 0; i < 100; i++) {
			timestamps[(int) i] = i;
		}

		Object[] valuesList = new Object[4];
		for (long i = 0; i < 4; i++) {
			Object[] values = new Object[100];
			for (long j = 0; j < 100; j++) {
				values[(int) j] = i + j;
			}
			valuesList[(int) i] = values;
		}

		List<DataType> dataTypeList = new ArrayList<>();
		for (int i = 0; i < 4; i++) {
			dataTypeList.add(DataType.LONG);
		}

		session.insertColumnRecords(paths, timestamps, valuesList, dataTypeList, null);
	}

	private static void insertRecords2() throws SessionException, ExecutionException {
		List<String> paths = new ArrayList<>();
		paths.add(S1);
		paths.add(S2);
		paths.add(S3);
		paths.add(S4);

		long[] timestamps = new long[100];
		for (long i = 0; i < 100; i++) {
			timestamps[(int) i] = i;
		}

		Object[] valuesList = new Object[4];
		for (long i = 0; i < 4; i++) {
			Object[] values = new Object[100];
			for (long j = 0; j < 100; j++) {
				values[(int) j] = i + j;
			}
			valuesList[(int) i] = values;
		}

		List<DataType> dataTypeList = new ArrayList<>();
		for (int i = 0; i < 4; i++) {
			dataTypeList.add(DataType.LONG);
		}

		session.insertColumnRecords(paths, timestamps, valuesList, dataTypeList, null);
	}

	private static void queryData() throws SessionException {
		List<String> paths = new ArrayList<>();
		paths.add(S1);
		paths.add(S2);
		paths.add(S3);
		paths.add(S4);

		long startTime = 5L;
		long endTime = 55L;

		SessionQueryDataSet dataSet = session.queryData(paths, startTime, endTime);
		dataSet.print();
	}

	private static void deleteDataInColumns() throws SessionException {
		List<String> paths = new ArrayList<>();
		paths.add(S1);
		paths.add(S2);
		paths.add(S3);

		long startTime = 25L;
		long endTime = 30L;

		session.deleteDataInColumns(paths, startTime, endTime);
	}

	private static void aggregateQuery() throws SessionException {
		List<String> paths = new ArrayList<>();
		paths.add(S1);
		paths.add(S2);
		paths.add(S3);
		paths.add(S4);

		long startTime = 5L;
		long endTime = 55L;

		SessionAggregateQueryDataSet dataSet = session.aggregateQuery(paths, startTime, endTime, AggregateType.MAX);
		dataSet.print();

		dataSet = session.aggregateQuery(paths, startTime, endTime, AggregateType.MIN);
		dataSet.print();

		dataSet = session.aggregateQuery(paths, startTime, endTime, AggregateType.FIRST);
		dataSet.print();

		dataSet = session.aggregateQuery(paths, startTime, endTime, AggregateType.LAST);
		dataSet.print();

		dataSet = session.aggregateQuery(paths, startTime, endTime, AggregateType.COUNT);
		dataSet.print();

		dataSet = session.aggregateQuery(paths, startTime, endTime, AggregateType.SUM);
		dataSet.print();

		dataSet = session.aggregateQuery(paths, startTime, endTime, AggregateType.AVG);
		dataSet.print();
	}
}
