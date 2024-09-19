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

include "rpc.thrift"
namespace java cn.edu.tsinghua.iginx.thrift

struct RawTagFilter {
    1: required rpc.TagFilterType type
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
    NOT_LIKE,
    GE_AND,
    G_AND,
    LE_AND,
    L_AND,
    E_AND,
    NE_AND,
    LIKE_AND,
    NOT_LIKE_AND,
    UNKNOWN,
}

struct RawValue {
    1: required rpc.DataType dataType
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

struct RawHeader {
    1: required list<string> names
    2: required list<rpc.DataType> types
    3: required list<map<string, string>> tagsList;
    4: required bool hasKey
}

struct RawRow {
    1: optional i64 key
    2: required binary rowValues
    3: required binary bitmap
}

struct RawKeyRange {
    1: required i64 beginKey;
    2: required bool includeBeginKey;
    3: required i64 endKey;
    4: required bool includeEndKey;
}

struct TS {
    1: required string path
    2: required rpc.DataType dataType
    3: optional map<string, string> tags
}

struct RawField {
    1: required string path
    2: required rpc.DataType dataType
    3: required map<string, string> tags
}

