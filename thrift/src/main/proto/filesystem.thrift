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

struct RawTagFilter {
    1: required TagFilterType type
    2: optional string key
    3: optional string value
    4: optional map<string, string> tags
    5: optional list<RawTagFilter> children
}

struct ProjectReq {
    1: required string storageUnit
    2: required bool isDummyStorageUnit
    3: required list<string> paths
    4: optional RawTagFilter tagFilter
    5: optional binary filter
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

struct GetStorageBoundryResp {
    1: required Status status
    2: optional i64 startTime
    3: optional i64 endTime
    4: optional string startTimeSeries
    5: optional string endTimeSeries
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

    GetStorageBoundryResp getBoundaryOfStorage(1: string prefix);

}