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

package cn.edu.tsinghua.iginx.engine.shared;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchStream;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.file.write.ExportCsv;
import cn.edu.tsinghua.iginx.exception.StatusCode;
import cn.edu.tsinghua.iginx.thrift.*;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import cn.edu.tsinghua.iginx.utils.ByteUtils;
import cn.edu.tsinghua.iginx.utils.RpcUtils;
import java.nio.ByteBuffer;
import java.util.*;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Data
public class Result {

  private static final Logger LOGGER = LoggerFactory.getLogger(Result.class);

  private Status status;
  private List<ByteBuffer> arrowData;

  private BatchStream batchStream;

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
  private List<Long> jobIdList;

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
    // TODO: refactor this part
    throw new UnsupportedOperationException("Not implemented yet");
    //    resp.setPaths(paths);
    //    resp.setTagsList(tagsList);
    //    resp.setDataTypeList(dataTypes);
    //    return resp;
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

    resp.setIginxInfos(iginxInfos);
    resp.setStorageEngineInfos(storageEngineInfos);
    resp.setMetaStorageInfos(metaStorageInfos);
    resp.setLocalMetaStorageInfo(localMetaStorageInfo);
    resp.setRegisterTaskInfos(registerTaskInfos);
    resp.setJobId(jobId);
    resp.setJobState(jobState);
    resp.setJobIdList(jobIdList);
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

  public ExecuteStatementResp getExecuteStatementResp(int fetchSize) {
    ExecuteStatementResp resp = new ExecuteStatementResp(status, sqlType);
    resp.setWarningMsg(status.getMessage());
    if (status != RpcUtils.SUCCESS && status.code != StatusCode.PARTIAL_SUCCESS.getStatusCode()) {
      return resp;
    }
    resp.setQueryId(queryId);

    // TODO: need to be refactored
    throw new UnsupportedOperationException("Not implemented yet");
    //  try {
    //      List<ByteBuffer> valuesList = new ArrayList<>();
    //      List<ByteBuffer> bitmapList = new ArrayList<>();
    //
    //      int cnt = 0;
    //      boolean hasKey = resultStream.getHeader().hasKey();
    //      while (resultStream.hasNext() && cnt < fetchSize) {
    //        Row row = resultStream.next();
    //
    //        Object[] rawValues = row.getValues();
    //        Object[] rowValues = rawValues;
    //        if (hasKey) {
    //          rowValues = new Object[rawValues.length + 1];
    //          rowValues[0] = row.getKey();
    //          System.arraycopy(rawValues, 0, rowValues, 1, rawValues.length);
    //        }
    //        valuesList.add(ByteUtils.getRowByteBuffer(rowValues, types));
    //
    //        Bitmap bitmap = new Bitmap(rowValues.length);
    //        for (int i = 0; i < rowValues.length; i++) {
    //          if (rowValues[i] != null) {
    //            bitmap.mark(i);
    //          }
    //        }
    //        bitmapList.add(ByteBuffer.wrap(bitmap.getBytes()));
    //        cnt++;
    //      }
    //
    //      resp.setColumns(paths);
    //      resp.setTagsList(tagsList);
    //      resp.setDataTypeList(types);
    //      // resp.setQueryDataSet(new QueryDataSetV2(valuesList, bitmapList));
    //
    //      // OUTFILE AS STREAM
    //      resp.setExportStreamDir(exportByteStreamDir);
    //
    //      // OUTFILE AS CSV
    //      if (exportCsv != null) {
    //        CSVFile csvFile = exportCsv.getCsvFile();
    //        resp.setExportCSV(
    //            new ExportCSV(
    //                exportCsv.getFilepath(),
    //                exportCsv.isExportHeader(),
    //                csvFile.getDelimiter(),
    //                csvFile.isOptionallyQuote(),
    //                (short) csvFile.getQuote(),
    //                (short) csvFile.getEscaped(),
    //                csvFile.getRecordSeparator()));
    //      }
    //    } catch (PhysicalException e) {
    //      LOGGER.error("unexpected error when load row stream: ", e);
    //      resp.setStatus(RpcUtils.FAILURE);
    //    }
    // return resp;
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

  public FetchResultsResp fetch(int fetchSize) {
    FetchResultsResp resp = new FetchResultsResp(status, false);

    if (status != RpcUtils.SUCCESS && status.code != StatusCode.PARTIAL_SUCCESS.getStatusCode()) {
      return resp;
    }
    try {
      List<DataType> types = new ArrayList<>();

      Header header = resultStream.getHeader();

      if (header.hasKey()) {
        types.add(Field.KEY.getType());
      }

      resultStream.getHeader().getFields().forEach(field -> types.add(field.getType()));

      List<ByteBuffer> valuesList = new ArrayList<>();
      List<ByteBuffer> bitmapList = new ArrayList<>();

      int cnt = 0;
      boolean hasKey = resultStream.getHeader().hasKey();
      while (resultStream.hasNext() && cnt < fetchSize) {
        Row row = resultStream.next();

        Object[] rawValues = row.getValues();
        Object[] rowValues = rawValues;
        if (hasKey) {
          rowValues = new Object[rawValues.length + 1];
          rowValues[0] = row.getKey();
          System.arraycopy(rawValues, 0, rowValues, 1, rawValues.length);
        }
        valuesList.add(ByteUtils.getRowByteBuffer(rowValues, types));

        Bitmap bitmap = new Bitmap(rowValues.length);
        for (int i = 0; i < rowValues.length; i++) {
          if (rowValues[i] != null) {
            bitmap.mark(i);
          }
        }
        bitmapList.add(ByteBuffer.wrap(bitmap.getBytes()));
        cnt++;
      }
      resp.setHasMoreResults(resultStream.hasNext());
      resp.setQueryDataSet(new QueryDataSetV2(valuesList, bitmapList));
    } catch (PhysicalException e) {
      LOGGER.error("unexpected error when load row stream: ", e);
      resp.setStatus(RpcUtils.FAILURE);
    }
    return resp;
  }
}
