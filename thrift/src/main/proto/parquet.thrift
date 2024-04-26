namespace java cn.edu.tsinghua.iginx.parquet.thrift

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

enum RawFilterType {
    Key,
    Value,
    Path,
    Bool,
    And,
    Or,
    Not,
}

enum RawFilterOp {
    GE,
    G,
    LE,
    L,
    E,
    NE,
    LIKE,
    GE_AND,
    G_AND,
    LE_AND,
    L_AND,
    E_AND,
    NE_AND,
    LIKE_AND,
    UNKNOWN,
}

struct RawValue {
    1: required string dataType
    2: optional bool boolV
    3: optional i32 intV
    4: optional i64 longV
    5: optional double floatV
    6: optional double doubleV
    7: optional binary binaryV
}

struct RawFilter {
    1: required RawFilterType type
    2: optional list<RawFilter> children
    3: optional bool isTrue
    4: optional i64 keyValue
    5: optional RawFilterOp op
    6: optional string pathA
    7: optional string pathB
    8: optional string path
    9: optional RawValue value
}

struct ProjectReq {
    1: required string storageUnit
    2: required bool isDummyStorageUnit
    3: required list<string> paths
    4: optional RawTagFilter tagFilter
    5: optional RawFilter filter
}

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
    1: required Status status
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
    3: optional RawTagFilter tagFilter
    4: optional list<ParquetKeyRange> keyRanges
}

struct GetStorageBoundaryResp {
    1: required Status status
    2: optional i64 startKey
    3: optional i64 endKey
    4: optional string startColumn
    5: optional string endColumn
}

struct TS {
    1: required string path
    2: required string dataType
    3: optional map<string, string> tags
}

struct GetColumnsOfStorageUnitResp {
    1: required Status status
    2: optional list<TS> tsList
}

service ParquetService {

    ProjectResp executeProject(1: ProjectReq req);

    Status executeInsert(1: InsertReq req);

    Status executeDelete(1: DeleteReq req);

    GetColumnsOfStorageUnitResp getColumnsOfStorageUnit(1: string storageUnit);

    GetStorageBoundaryResp getBoundaryOfStorage(1: string dataPrefix);

}