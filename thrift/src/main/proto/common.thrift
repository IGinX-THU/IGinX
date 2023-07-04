include "rpc.thrift"
namespace java cn.edu.tsinghua.iginx.common.thrift

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