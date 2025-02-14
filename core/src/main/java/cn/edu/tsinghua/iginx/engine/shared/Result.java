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
package cn.edu.tsinghua.iginx.engine.shared;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.VectorSchemaRoots;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.util.Batch;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchStream;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.file.CSVFile;
import cn.edu.tsinghua.iginx.engine.shared.file.write.ExportCsv;
import cn.edu.tsinghua.iginx.exception.StatusCode;
import cn.edu.tsinghua.iginx.thrift.*;
import cn.edu.tsinghua.iginx.utils.RpcUtils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import lombok.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.util.VectorSchemaRootAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Data
public class Result {

  private static final Logger LOGGER = LoggerFactory.getLogger(Result.class);

  private Status status;
  private List<ByteBuffer> arrowData;

  private List<String> paths;
  private List<DataType> dataTypes;

  private BatchStream batchStream;
  private static VectorSchemaRoot streamCache;

  private SqlType sqlType;
  private Long pointsNum;
  private Integer replicaNum;

  private List<IginxInfo> iginxInfos;
  private List<StorageEngineInfo> storageEngineInfos;
  private List<MetaStorageInfo> metaStorageInfos;
  private LocalMetaStorageInfo localMetaStorageInfo;

  private List<RegisterTaskInfo> registerTaskInfos;

  private long queryId;
  private JobState jobState;
  private RowStream resultStream;

  private long jobId;
  private Map<JobState, List<Long>> jobStateMap;
  private String jobYamlPath;

  private Map<String, String> configs;

  private String exportByteStreamDir;

  private ExportCsv exportCsv;

  private String loadCSVPath;
  private List<String> loadCSVColumns;
  private Long loadCSVRecordNum;

  private String UDFModulePath;

  private List<Long> sessionIDs;

  private Map<String, Boolean> rules;

  private List<String> usernames;
  private List<UserType> userTypes;
  private List<Set<AuthType>> auths;

  public Result(Status status) {
    this.status = status;
    this.pointsNum = 0L;
    this.replicaNum = 0;
  }

  public QueryDataResp getQueryDataResp() {
    QueryDataResp resp = new QueryDataResp(status);
    resp.setQueryArrowData(arrowData);
    return resp;
  }

  public AggregateQueryResp getAggregateQueryResp() {
    AggregateQueryResp resp = new AggregateQueryResp(status);
    resp.setQueryArrowData(arrowData);
    return resp;
  }

  public DownsampleQueryResp getDownSampleQueryResp() {
    DownsampleQueryResp resp = new DownsampleQueryResp(status);
    resp.setQueryArrowData(arrowData);
    return resp;
  }

  public LastQueryResp getLastQueryResp() {
    LastQueryResp resp = new LastQueryResp(status);
    resp.setQueryArrowData(arrowData);
    return resp;
  }

  public ShowColumnsResp getShowColumnsResp() {
    ShowColumnsResp resp = new ShowColumnsResp(status);
    resp.setPaths(paths);
    resp.setDataTypeList(dataTypes);
    return resp;
  }

  public ExecuteSqlResp getExecuteSqlResp() {
    ExecuteSqlResp resp = new ExecuteSqlResp(status, sqlType);
    if (status != RpcUtils.SUCCESS && status.code != StatusCode.PARTIAL_SUCCESS.getStatusCode()) {
      resp.setParseErrorMsg(status.getMessage());
      return resp;
    }

    resp.setReplicaNum(replicaNum);
    resp.setPointsNum(pointsNum);
    resp.setQueryArrowData(arrowData);

    resp.setPaths(paths);
    resp.setDataTypeList(dataTypes);

    resp.setIginxInfos(iginxInfos);
    resp.setStorageEngineInfos(storageEngineInfos);
    resp.setMetaStorageInfos(metaStorageInfos);
    resp.setLocalMetaStorageInfo(localMetaStorageInfo);
    resp.setRegisterTaskInfos(registerTaskInfos);
    resp.setJobId(jobId);
    resp.setJobState(jobState);
    resp.setJobStateMap(jobStateMap);
    resp.setJobYamlPath(jobYamlPath);
    resp.setConfigs(configs);
    // INFILE AS CSV
    resp.setLoadCsvPath(loadCSVPath);
    resp.setSessionIDList(sessionIDs);
    resp.setRules(rules);
    // import udf
    resp.setUDFModulePath(UDFModulePath);
    // SHOW USER
    resp.setUsernames(usernames);
    resp.setUserTypes(userTypes);
    resp.setAuths(auths);
    return resp;
  }

  public ExecuteStatementResp getExecuteStatementResp(BufferAllocator allocator, int fetchSize) {
    ExecuteStatementResp resp = new ExecuteStatementResp(status, sqlType);
    resp.setWarningMsg(status.getMessage());
    if (status != RpcUtils.SUCCESS && status.code != StatusCode.PARTIAL_SUCCESS.getStatusCode()) {
      return resp;
    }
    resp.setQueryId(queryId);

    try {
      resp.setQueryArrowData(getArrowDataFromStream(allocator, fetchSize));
      // OUTFILE AS STREAM
      resp.setExportStreamDir(exportByteStreamDir);

      // OUTFILE AS CSV
      if (exportCsv != null) {
        CSVFile csvFile = exportCsv.getCsvFile();
        resp.setExportCSV(
            new ExportCSV(
                exportCsv.getFilepath(),
                exportCsv.isExportHeader(),
                csvFile.getDelimiter(),
                csvFile.isOptionallyQuote(),
                (short) csvFile.getQuote(),
                (short) csvFile.getEscaped(),
                csvFile.getRecordSeparator()));
      }
    } catch (IOException | PhysicalException e) {
      LOGGER.error("unexpected error when load row stream: ", e);
      resp.setStatus(RpcUtils.FAILURE);
    }

    return resp;
  }

  public LoadCSVResp getLoadCSVResp() {
    LoadCSVResp resp = new LoadCSVResp(status);
    if (status != RpcUtils.SUCCESS && status.code != StatusCode.PARTIAL_SUCCESS.getStatusCode()) {
      resp.setParseErrorMsg(status.getMessage());
      return resp;
    }
    resp.setColumns(loadCSVColumns);
    resp.setRecordsNum(loadCSVRecordNum);
    return resp;
  }

  public LoadUDFResp getLoadUDFResp() {
    LoadUDFResp resp = new LoadUDFResp(status);

    if (status != RpcUtils.SUCCESS && status.code != StatusCode.PARTIAL_SUCCESS.getStatusCode()) {
      resp.setParseErrorMsg(status.getMessage());
      return resp;
    }
    resp.setUDFModulePath(UDFModulePath);
    return resp;
  }

  public FetchResultsResp fetch(BufferAllocator allocator, int fetchSize) {
    FetchResultsResp resp = new FetchResultsResp(status, false);

    if (status != RpcUtils.SUCCESS && status.code != StatusCode.PARTIAL_SUCCESS.getStatusCode()) {
      return resp;
    }

    try {
      resp.setHasMoreResults(batchStream.hasNext());
      resp.setQueryArrowData(getArrowDataFromStream(allocator, fetchSize));
    } catch (IOException | PhysicalException e) {
      LOGGER.error("unexpected error when load row stream: ", e);
      resp.setStatus(RpcUtils.FAILURE);
    }
    return resp;
  }

  private List<ByteBuffer> getArrowDataFromStream(BufferAllocator allocator, int fetchSize)
      throws PhysicalException, IOException {
    if (!batchStream.hasNext()) {
      return null;
    }
    List<ByteBuffer> dataList = new ArrayList<>();
    try {
      if (streamCache == null) {
        // transfer后，原root可以关闭
        try (Batch batch = batchStream.getNext()) {
          streamCache = VectorSchemaRoots.transfer(allocator, batch.getData());
        }
      }
      int count = streamCache.getRowCount();
      while (count < fetchSize && batchStream.hasNext()) {
        try (Batch batch = batchStream.getNext()) {
          VectorSchemaRoots.append(streamCache, batch.getData());
          count += batch.getRowCount();
        }
      }

      VectorSchemaRoot output, rest;
      if (count <= fetchSize) {
        output = streamCache;
        rest = null;
      } else {
        // 深拷贝，分为本次需要的数据部分和留待下次fetch的数据部分
        output = VectorSchemaRoot.create(batchStream.getSchema().raw(), allocator);
        VectorSchemaRootAppender.append(output, streamCache.slice(fetchSize));
        rest = VectorSchemaRoot.create(batchStream.getSchema().raw(), allocator);
        VectorSchemaRootAppender.append(
            rest, streamCache.slice(fetchSize, streamCache.getRowCount() - fetchSize));
      }

      try (ByteArrayOutputStream out = new ByteArrayOutputStream();
          ArrowStreamWriter writer = new ArrowStreamWriter(output, null, out)) {
        writer.start();
        writer.writeBatch();
        writer.close();
        dataList.add(ByteBuffer.wrap(out.toByteArray()));
      }

      streamCache = rest;

    } catch (PhysicalException e) {
      LOGGER.error("Failed to get data from stream", e);
      throw e;
    } catch (IOException e) {
      LOGGER.error("Failed to write byte stream", e);
      throw e;
    }
    return dataList;
  }
}
