include "common.thrift"
namespace java cn.edu.tsinghua.iginx.parquet.thrift

struct ParquetHeader {
    1: required list<string> names
    2: required list<string> types
    3: required list<map<string, string>> tagsList;
    4: required bool hasKey
}

struct ParquetRow {
    1: optional i64 key
    2: required binary rowValues
    3: required binary bitmap
}

struct ProjectResp {
    1: required common.Status status
    2: optional ParquetHeader header
    3: optional list<ParquetRow> rows
}

struct ParquetRawData {
    1: required list<string> paths
    2: required list<map<string, string>> tagsList
    3: required binary keys
    4: required list<binary> valuesList
    5: required list<binary> bitmapList
    6: required list<string> dataTypeList
    7: required string rawDataType
}

struct InsertReq {
    1: required string storageUnit
    2: required ParquetRawData rawData;
}

struct ParquetKeyRange {
    1: required i64 beginKey;
    2: required bool includeBeginKey;
    3: required i64 endKey;
    4: required bool includeEndKey;
}

struct DeleteReq {
    1: required string storageUnit
    2: required list<string> paths
    3: optional common.RawTagFilter tagFilter
    4: optional list<ParquetTimeRange> timeRanges
}

struct TS {
    1: required string path
    2: required string dataType
    3: optional map<string, string> tags
}

struct GetTimeSeriesOfStorageUnitResp {
    1: required common.Status status
    2: optional list<TS> tsList
}

service ParquetService {

    ProjectResp executeProject(1: common.ProjectReq req);

    common.Status executeInsert(1: InsertReq req);

    common.Status executeDelete(1: DeleteReq req);

    GetColumnsOfStorageUnitResp getColumnsOfStorageUnit(1: string storageUnit);

    common.GetStorageBoundryResp getBoundaryOfStorage();

}