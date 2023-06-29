include "common.thrift"
namespace java cn.edu.tsinghua.iginx.filesystem.thrift

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
    1: required common.Status status
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
    3: optional common.RawTagFilter tagFilter
    4: optional list<FileSystemTimeRange> timeRanges
}

struct PathSet {
    1: required string path
    2: required string dataType
    3: optional map<string, string> tags
}

struct GetTimeSeriesOfStorageUnitResp {
    1: required common.Status status
    2: optional list<PathSet> PathList
}

service FileSystemService {

    ProjectResp executeProject(1: common.ProjectReq req);

    common.Status executeInsert(1: InsertReq req);

    common.Status executeDelete(1: DeleteReq req);

    GetTimeSeriesOfStorageUnitResp getTimeSeriesOfStorageUnit(1: string storageUnit);

    common.GetStorageBoundaryResp getBoundaryOfStorage(1: string prefix);

}