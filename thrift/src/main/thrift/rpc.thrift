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
namespace java cn.edu.tsinghua.iginx.thrift
namespace py iginx.thrift.rpc

enum DataType {
    BOOLEAN,
    INTEGER,
    LONG,
    FLOAT,
    DOUBLE,
    BINARY,
}

enum StorageEngineType {
    unknown,
    iotdb12,
    influxdb,
    filesystem,
    relational,
    mongodb,
    redis
}

enum AggregateType {
    MAX,
    MIN,
    SUM,
    COUNT,
    AVG,
    FIRST_VALUE,
    LAST_VALUE,
    FIRST,
    LAST
}

enum SqlType {
    Unknown,
    Insert,
    Delete,
    Query,
    GetReplicaNum,
    AddStorageEngines,
    AlterStorageEngine,
    CountPoints,
    ClearData,
    DeleteColumns,
    ShowColumns,
    ShowClusterInfo,
    ShowRegisterTask,
    RegisterTask,
    DropTask,
    CommitTransformJob,
    ShowJobStatus,
    CancelJob,
    ShowEligibleJob,
    RemoveStorageEngine,
    SetConfig,
    ShowConfig,
    Compact,
    ExportCsv,
    ExportStream,
    LoadCsv,
    ShowSessionID,
    ShowRules,
    SetRules,
    CreateUser,
    GrantUser,
    ChangeUserPassword,
    DropUser,
    ShowUser,
}

enum AuthType {
    Read,
    Write,
    Admin,
    Cluster
}

enum UserType {
    Administrator,
    OrdinaryUser
}

enum ExportType {
    LOG,
    FILE,
    IGINX
}

enum TaskType {
    IGINX,
    PYTHON
}

enum DataFlowType {
    BATCH,
    STREAM
}

enum JobState {
    JOB_UNKNOWN,
    JOB_FINISHED,
    JOB_CREATED,
    JOB_IDLE,
    JOB_RUNNING,
    JOB_PARTIALLY_FAILING,
    JOB_PARTIALLY_FAILED,
    JOB_FAILING,
    JOB_FAILED,
    JOB_CLOSING,
    JOB_CLOSED
}

enum UDFType {
    UDAF,
    UDTF,
    UDSF,
    TRANSFORM
}

enum TimePrecision {
    YEAR,
    MONTH,
    WEEK,
    DAY,
    HOUR,
    MIN,
    S,
    MS,
    US,
    NS
}

enum TagFilterType {
    Base,
    And,
    Or,
    BasePrecise,
    Precise,
    WithoutTag,
}

struct Status {
    1: required i32 code
    2: optional string message
    3: optional list<Status> subStatus
}

struct UDFClassPair {
    1: required string name
    2: required string classPath
}

struct OpenSessionReq {
    1: optional string username
    2: optional string password
}

struct OpenSessionResp {
    1: required Status status
    2: optional i64 sessionId
}

struct CloseSessionReq {
    1: required i64 sessionId
}

struct DeleteColumnsReq {
    1: required i64 sessionId
    2: required list<string> paths
    3: optional list<map<string, list<string>>> tagsList
    4: optional TagFilterType filterType
}

struct InsertColumnRecordsReq {
    1: required i64 sessionId
    2: required list<string> paths
    3: required binary keys
    4: required list<binary> valuesList
    5: required list<binary> bitmapList
    6: required list<DataType> dataTypeList
    7: optional list<map<string, string>> tagsList
    8: optional TimePrecision timePrecision
}

struct InsertNonAlignedColumnRecordsReq {
    1: required i64 sessionId
    2: required list<string> paths
    3: required binary keys
    4: required list<binary> valuesList
    5: required list<binary> bitmapList
    6: required list<DataType> dataTypeList
    7: optional list<map<string, string>> tagsList
    8: optional TimePrecision timePrecision
}

struct InsertRowRecordsReq {
    1: required i64 sessionId
    2: required list<string> paths
    3: required binary keys
    4: required list<binary> valuesList
    5: required list<binary> bitmapList
    6: required list<DataType> dataTypeList
    7: optional list<map<string, string>> tagsList
    8: optional TimePrecision timePrecision
}

struct InsertNonAlignedRowRecordsReq {
    1: required i64 sessionId
    2: required list<string> paths
    3: required binary keys
    4: required list<binary> valuesList
    5: required list<binary> bitmapList
    6: required list<DataType> dataTypeList
    7: optional list<map<string, string>> tagsList
    8: optional TimePrecision timePrecision
}

struct DeleteDataInColumnsReq {
    1: required i64 sessionId
    2: required list<string> paths
    3: required i64 startKey
    4: required i64 endKey
    5: optional list<map<string, list<string>>> tagsList
    6: optional TagFilterType filterType
    7: optional TimePrecision timePrecision
}

struct QueryDataSet {
    1: required binary keys
    2: required list<binary> valuesList
    3: required list<binary> bitmapList
}

struct QueryDataReq {
    1: required i64 sessionId
    2: required list<string> paths
    3: required i64 startKey
    4: required i64 endKey
    5: optional list<map<string, list<string>>> tagsList
    6: optional TimePrecision timePrecision
    7: optional TagFilterType filterType
}

struct QueryDataResp {
    1: required Status status
    2: optional list<string> paths
    3: optional list<map<string, string>> tagsList
    4: optional list<DataType> dataTypeList
    5: optional QueryDataSet queryDataSet
}

struct AddStorageEnginesReq {
    1: required i64 sessionId
    2: required list<StorageEngine> storageEngines
}

struct AlterStorageEngineReq {
    1: required i64 sessionId
    2: required i64 engineId
    3: required map<string, string> newParams
}

struct StorageEngine {
    1: required string ip
    2: required i32 port
    3: required StorageEngineType type
    4: required map<string, string> extraParams
}

struct AggregateQueryReq {
    1: required i64 sessionId
    2: required list<string> paths
    3: required i64 startKey
    4: required i64 endKey
    5: required AggregateType aggregateType
    6: optional list<map<string, list<string>>> tagsList
    7: optional TimePrecision timePrecision
    8: optional TagFilterType filterType
}

struct AggregateQueryResp {
    1: required Status status
    2: optional list<string> paths
    3: optional list<map<string, string>> tagsList
    4: optional list<DataType> dataTypeList
    5: optional binary keys
    6: optional binary valuesList
}

struct LastQueryReq {
    1: required i64 sessionId
    2: required list<string> paths
    3: required i64 startKey
    4: optional list<map<string, list<string>>> tagsList
    5: optional TimePrecision timePrecision
    6: optional TagFilterType filterType
}

struct LastQueryResp {
    1: required Status status
    2: optional list<string> paths
    3: optional list<map<string, string>> tagsList
    4: optional list<DataType> dataTypeList
    5: optional QueryDataSet queryDataSet
}

struct DownsampleQueryReq {
    1: required i64 sessionId
    2: required list<string> paths
    3: required i64 startKey
    4: required i64 endKey
    5: required AggregateType aggregateType
    6: required i64 precision
    7: optional list<map<string, list<string>>> tagsList
    8: optional TimePrecision timePrecision
    9: optional TagFilterType filterType
}

struct DownsampleQueryResp {
    1: required Status status
    2: optional list<string> paths
    3: optional list<map<string, string>> tagsList
    4: optional list<DataType> dataTypeList
    5: optional QueryDataSet queryDataSet
}

struct ShowColumnsReq {
    1: required i64 sessionId
}

struct ShowColumnsResp {
    1: required Status status
    2: optional list<string> paths
    3: optional list<map<string, string>> tagsList
    4: optional list<DataType> dataTypeList
}

struct GetReplicaNumReq {
    1: required i64 sessionId
}

struct GetReplicaNumResp {
    1: required Status status
    2: optional i32 replicaNum
}


struct ExecuteSqlReq {
    1: required i64 sessionId
    2: required string statement
    3: optional bool remoteSession
}

struct ExecuteSqlResp {
    1: required Status status
    2: required SqlType type
    3: optional list<string> paths
    4: optional list<map<string, string>> tagsList
    5: optional list<DataType> dataTypeList
    6: optional QueryDataSet queryDataSet
    7: optional binary keys
    8: optional binary valuesList
    9: optional i32 replicaNum
    10: optional i64 pointsNum;
    11: optional AggregateType aggregateType
    12: optional string parseErrorMsg
    13: optional i32 limit
    14: optional i32 offset
    15: optional string orderByPath
    16: optional bool ascending
    17: optional list<IginxInfo> iginxInfos
    18: optional list<StorageEngineInfo> storageEngineInfos
    19: optional list<MetaStorageInfo>  metaStorageInfos
    20: optional LocalMetaStorageInfo localMetaStorageInfo
    21: optional list<RegisterTaskInfo> registerTaskInfos
    22: optional i64 jobId
    23: optional JobState jobState
    24: optional map<JobState, list<i64>> jobStateMap
    25: optional string jobYamlPath
    26: optional map<string, string> configs
    27: optional string loadCsvPath
    28: optional list<i64> sessionIDList
    29: optional map<string, bool> rules
    30: optional string UDFModulePath
    31: optional list<string> usernames
    32: optional list<UserType> userTypes
    33: optional list<set<AuthType>> auths
}

struct UpdateUserReq {
    1: required i64 sessionId
    2: required string username
    3: optional string password
    4: optional set<AuthType> auths
}

struct AddUserReq {
    1: required i64 sessionId
    2: required string username
    3: required string password
    4: required set<AuthType> auths
}

struct DeleteUserReq {
    1: required i64 sessionId
    2: required string username
}

struct GetUserReq {
    1: required i64 sessionId
    2: optional list<string> usernames
}

struct GetUserResp {
    1: required Status status
    2: optional list<string> usernames
    3: optional list<UserType> userTypes
    4: optional list<set<AuthType>> auths
}

struct GetClusterInfoReq {
    1: required i64 sessionId
}

struct IginxInfo {
    1: required i64 id
    2: required string ip
    3: required i32 port
}

struct StorageEngineInfo {
    1: required i64 id
    2: required string ip
    3: required i32 port
    4: required StorageEngineType type
    5: optional string schemaPrefix
    6: optional string dataPrefix
}

struct MetaStorageInfo {
    1: required string ip
    2: required i32 port
    3: required string type
}

struct LocalMetaStorageInfo {
    1: required string path
}

struct GetClusterInfoResp {
    1: required Status status
    2: optional list<IginxInfo> iginxInfos
    3: optional list<StorageEngineInfo> storageEngineInfos
    4: optional list<MetaStorageInfo>  metaStorageInfos
    5: optional LocalMetaStorageInfo localMetaStorageInfo
}

struct ExecuteStatementReq {
    1: required i64 sessionId
    2: required string statement
    3: optional i32 fetchSize
    4: optional i64 timeout
}

struct ExecuteStatementResp {
    1: required Status status
    2: required SqlType type
    3: optional i64 queryId
    4: optional list<string> columns
    5: optional list<map<string, string>> tagsList
    6: optional list<DataType> dataTypeList
    7: optional QueryDataSetV2 queryDataSet
    8: optional string warningMsg;
    9: optional string exportStreamDir
    10: optional ExportCSV exportCSV
}

struct ExportCSV {
    1: required string exportCsvPath
    2: required bool isExportHeader
    3: required string delimiter
    4: required bool isOptionallyQuote
    5: required i16 quote
    6: required i16 escaped
    7: required string recordSeparator
}

struct QueryDataSetV2 {
    1: required list<binary> valuesList
    2: required list<binary> bitmapList
}

struct CloseStatementReq {
    1: required i64 sessionId
    2: required i64 queryId
}

struct FetchResultsReq {
    1: required i64 sessionId
    2: required i64 queryId
    3: optional i32 fetchSize
    4: optional i64 timeout
}

struct FetchResultsResp {
    1: required Status status
    2: required bool hasMoreResults
    3: optional QueryDataSetV2 queryDataSet
}

struct LoadCSVReq {
    1: required i64 sessionId
    2: required string statement
    3: required string csvFileName
}

struct LoadCSVResp {
    1: required Status status
    2: optional list<string> columns
    3: optional i64 recordsNum
    4: optional string parseErrorMsg
}

struct LoadUDFReq {
    1: required i64 sessionId
    2: required string statement
    3: optional binary udfFile
    4: required bool isRemote
}

struct LoadUDFResp {
    1: required Status status
    2: optional string parseErrorMsg
    3: optional string UDFModulePath
}

struct TaskInfo {
    1: required TaskType taskType
    2: required DataFlowType dataFlowType
    3: optional i64 timeout
    4: optional list<string> sqlList
    5: optional string pyTaskName
}

struct CommitTransformJobReq {
    1: required i64 sessionId
    2: required list<TaskInfo> taskList
    3: required ExportType exportType
    4: optional string fileName
    5: optional string schedule
    6: optional bool stopOnFailure
    7: optional Notification notification
}

struct Notification {
    1: optional Email email
}

struct Email {
    1: required string hostName
    2: required string smtpPort
    3: required string username
    4: required string password
    5: required string fromAddr
    6: required list<string> toAddrs
}

struct CommitTransformJobResp {
    1: required Status status
    2: required i64 jobId
}

struct QueryTransformJobStatusReq {
    1: required i64 sessionId
    2: required i64 jobId
}

struct QueryTransformJobStatusResp {
    1: required Status status
    2: required JobState jobState
}

struct ShowEligibleJobReq {
    1: required i64 sessionId
    2: optional JobState jobState
}

struct ShowEligibleJobResp {
    1: required Status status
    2: required map<JobState, list<i64>> jobStateMap
}

struct CancelTransformJobReq {
    1: required i64 sessionId
    2: required i64 jobId
}

struct RegisterTaskReq {
    1: required i64 sessionId
    2: required string filePath
    3: required list<UDFClassPair> UDFClassPairs
    4: required list<UDFType> types
    5: required binary moduleFile
    6: required bool isRemote
}

struct DropTaskReq {
    1: required i64 sessionId
    2: required string name
}

struct GetRegisterTaskInfoReq {
    1: required i64 sessionId
}

struct IpPortPair {
    1: required string ip
    2: required i32 port
}

struct RegisterTaskInfo {
    1: required string name
    2: required string className
    3: required string fileName
    4: required list<IpPortPair> ipPortPair
    5: required UDFType type;
}

struct GetRegisterTaskInfoResp {
    1: required Status status
    2: optional list<RegisterTaskInfo> registerTaskInfoList
}

struct CurveMatchReq {
    1: required i64 sessionId
    2: required list<string> paths
    3: required i64 startKey
    4: required i64 endKey
    5: required list<double> curveQuery
    6: required i64 curveUnit
}

struct CurveMatchResp {
    1: required Status status
    2: optional string matchedPath
    3: optional i64 matchedKey
}

enum DebugInfoType {
    GET_META,
}

struct GetMetaReq {
    1: required bool byCache
}

struct Fragment {
    1: required string storageUnitId
    2: required i64 startKey
    3: required i64 endKey
    4: required string startTs
    5: required string endTs
}

struct Storage {
    1: required i64 id
    2: required string ip
    3: required i64 port
    4: required StorageEngineType type
}

struct StorageUnit {
    1: required string id
    2: required string masterId
    3: required i64 storageId
}

struct GetMetaResp {
    1: required list<Fragment> fragments
    2: required list<Storage> storages
    3: required list<StorageUnit> storageUnits
}

struct DebugInfoReq {
    1: required DebugInfoType payloadType
    2: required binary payload
}

struct DebugInfoResp {
    1: required Status status
    2: optional binary payload
}

struct RemovedStorageEngineInfo {
    1: required string ip
    2: required i32 port
    3: required string schemaPrefix
    4: required string dataPrefix
}

struct RemoveStorageEngineReq {
    1: required i64 sessionId
    2: required list<RemovedStorageEngineInfo> removedStorageEngineInfoList
}

struct ShowSessionIDReq {
    1: required i64 sessionId
}

struct ShowSessionIDResp {
    1: required Status status
    2: required list<i64> sessionIDList
}

struct ShowRulesReq {
    1: required i64 sessionId
}

struct ShowRulesResp {
    1: required Status status
    2: required map<string, bool> rules
}

struct SetRulesReq {
    1: required i64 sessionId
    2: required map<string, bool> rulesChange
}

struct FileChunk {
    1: required string fileName;
    2: required i64 offset;
    3: required binary data;
    4: required i64 chunkSize;
}

struct UploadFileReq {
    1: required i64 sessionId
    2: required FileChunk fileChunk
}

struct UploadFileResp {
    1: required Status status
}

service IService {

    OpenSessionResp openSession(1: OpenSessionReq req);

    Status closeSession(1: CloseSessionReq req);

    Status deleteColumns(1: DeleteColumnsReq req);

    Status insertColumnRecords(1: InsertColumnRecordsReq req);

    Status insertNonAlignedColumnRecords(1: InsertNonAlignedColumnRecordsReq req);

    Status insertRowRecords(1: InsertRowRecordsReq req);

    Status insertNonAlignedRowRecords(1: InsertNonAlignedRowRecordsReq req);

    Status deleteDataInColumns(1: DeleteDataInColumnsReq req);

    QueryDataResp queryData(1: QueryDataReq req);

    Status addStorageEngines(1: AddStorageEnginesReq req);

    Status alterStorageEngine(1: AlterStorageEngineReq req);

    Status removeStorageEngine(1: RemoveStorageEngineReq req);

    AggregateQueryResp aggregateQuery(1: AggregateQueryReq req);

    LastQueryResp lastQuery(1: LastQueryReq req);

    DownsampleQueryResp downsampleQuery(1: DownsampleQueryReq req);

    ShowColumnsResp showColumns(1: ShowColumnsReq req);

    GetReplicaNumResp getReplicaNum(1: GetReplicaNumReq req);

    ExecuteSqlResp executeSql(1: ExecuteSqlReq req);

    Status updateUser(1: UpdateUserReq req);

    Status addUser(1: AddUserReq req);

    Status deleteUser(1: DeleteUserReq req);

    GetUserResp getUser(1: GetUserReq req);

    GetClusterInfoResp getClusterInfo(1: GetClusterInfoReq req);

    ExecuteStatementResp executeStatement(1: ExecuteStatementReq req);

    FetchResultsResp fetchResults(1: FetchResultsReq req);

    LoadCSVResp loadCSV(1: LoadCSVReq req);

    LoadUDFResp loadUDF(1: LoadUDFReq req);

    Status closeStatement(1: CloseStatementReq req);

    CommitTransformJobResp commitTransformJob(1: CommitTransformJobReq req);

    QueryTransformJobStatusResp queryTransformJobStatus(1: QueryTransformJobStatusReq req);

    ShowEligibleJobResp showEligibleJob(1: ShowEligibleJobReq req);

    Status cancelTransformJob (1: CancelTransformJobReq req);

    Status registerTask(1: RegisterTaskReq req);

    Status dropTask(1: DropTaskReq req);

    GetRegisterTaskInfoResp getRegisterTaskInfo(1: GetRegisterTaskInfoReq req);

    CurveMatchResp curveMatch(1: CurveMatchReq req);

    DebugInfoResp debugInfo(1: DebugInfoReq req);

    ShowSessionIDResp showSessionID(1: ShowSessionIDReq req);

    ShowRulesResp showRules(1: ShowRulesReq req);

    Status setRules(1: SetRulesReq req);

    UploadFileResp uploadFileChunk(1: UploadFileReq req);
}
