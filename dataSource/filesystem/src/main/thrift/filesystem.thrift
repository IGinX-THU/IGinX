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
include "core.thrift"
namespace java cn.edu.tsinghua.iginx.filesystem.thrift

enum Status {
    OK,
    FileSystemException,
    UnknownException,
}

exception RpcException {
    1: required Status status
    2: required string message
}

struct DataUnit {
    1: required bool dummy
    2: optional string name
}

struct DataBoundary {
    1: optional string startColumn
    2: optional string endColumn
    3: required i64 startKey
    4: required i64 endKey
}

struct RawDataTarget {
    1: optional list<string> patterns
    2: optional core.RawTagFilter tagFilter
    3: optional core.RawFilter filter
}

struct RawDataSet {
    1: required core.RawHeader header
    2: required list<core.RawRow> rows
}

struct RawPrefix {
    1: optional string prefix
}

struct RawAggregate {
    1: optional rpc.AggregateType type
}

struct RawInserted {
    1: required list<string> patterns
    2: required list<map<string, string>> tagsList
    3: required binary keys
    4: required list<binary> valuesList
    5: required list<binary> bitmapList
    6: required list<string> dataTypeList
    7: required string rawDataType
}

service FileSystemRpc {
    map<DataUnit,DataBoundary> getUnits (1: RawPrefix prefix) throws (1: RpcException e);

    RawDataSet query(1: DataUnit unit, 2: RawDataTarget target, 3: RawAggregate aggregate) throws (1: RpcException e);

    void delete(1: DataUnit unit, 2: RawDataTarget target) throws (1: RpcException e);

    void insert(1: DataUnit unit, 2: RawInserted data) throws (1: RpcException e);
}