namespace java cn.edu.tsinghua.iginx.filesystem.thrift

struct Status {
    1: required i32 code
    2: required string message
}

enum TagFilterType {
    Base,
    And,
    Or,
    BasePrecise,
    Precise,
    WithoutTag,
}

enum FilterType {
    Key,
    Value,
    Path,
    Bool,
    And,
    Or,
    Not,
}

enum Op {
    GE,
    G,
    LE,
    L,
    E,
    NE,
    LIKE,
    UNKNOW,
}

struct Value {
    1: required rpc.DataType dataType
    2: optional bool boolV
    3: optional i32 intV
    4: optional i64 longV
    5: optional double floatV
    6: optional double doubleV
    7: optional binary binaryV
}

struct RawTagFilter {
    1: required TagFilterType type
    2: optional string key
    3: optional string value
    4: optional map<string, string> tags
    5: optional list<RawTagFilter> children
}

struct GetStorageBoundaryResp {
    1: required Status status
    2: optional i64 startKey
    3: optional i64 endKey
    4: optional string startColumn
    5: optional string endColumn
}

struct FSFilter {
    1: required FilterType type
    2: optional list<FSFilter> children
    3: optional bool isTrue
    4: optional i64 keyValue
    5: optional Op op
    6: optional string pathA
    7: optional string pathB
    8: optional string path
    9: optional Value value
}

struct FileDataHeader {
    1: required list<string> names
    2: required list<string> types
    3: required list<map<string, string>> tagsList
    4: required bool hasTime
}

struct FileDataRow {
    1: optional i64 timestamp
    2: required binary rowValues
    3: required binary bitmap
}

struct ProjectReq {
    1: required string storageUnit
    2: required bool isDummyStorageUnit
    3: required list<string> paths
    4: optional RawTagFilter tagFilter
    5: optional string filter
}

struct ProjectResp {
    1: required Status status
    2: optional FileDataHeader header
    3: optional list<FileDataRow> rows
}

struct FileDataRawData {
    1: required list<string> paths
    2: required list<map<string, string>> tagsList
    3: required binary timestamps
    4: required list<binary> valuesList
    5: required list<binary> bitmapList
    6: required list<string> dataTypeList
    7: required string rawDataType
}

struct InsertReq {
    1: required string storageUnit
    2: required FileDataRawData rawData;
}

struct FileSystemTimeRange {
    1: required i64 beginTime;
    2: required bool includeBeginTime;
    3: required i64 endTime;
    4: required bool includeEndTime;
}

struct DeleteReq {
    1: required string storageUnit
    2: required list<string> paths
    3: optional RawTagFilter tagFilter
    4: optional list<FileSystemTimeRange> timeRanges
}

struct PathSet {
    1: required string path
    2: required string dataType
    3: optional map<string, string> tags
}

struct GetTimeSeriesOfStorageUnitResp {
    1: required Status status
    2: optional list<PathSet> PathList
}

service FileSystemService {

    ProjectResp executeProject(1: ProjectReq req);

    Status executeInsert(1: InsertReq req);

    Status executeDelete(1: DeleteReq req);

    GetTimeSeriesOfStorageUnitResp getTimeSeriesOfStorageUnit(1: string storageUnit);

    GetStorageBoundaryResp getBoundaryOfStorage(1: string prefix);

}