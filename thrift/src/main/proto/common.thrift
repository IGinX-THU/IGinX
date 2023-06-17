namespace java cn.edu.tsinghua.iginx.common.thrift

struct Status {
    1: required 32 code
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