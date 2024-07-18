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

include "core.thrift"
namespace java cn.edu.tsinghua.iginx.filestore.rpc

struct Status {
    1: required i32 code
    2: required string message
}

struct ProjectReq {
    1: required string storageUnit
    2: required list<string> patterns
    3: optional core.RawTagFilter tagFilter
    4: optional core.RawFilter filter
    5: optional list<core.RawFunctionCall> aggregations
}

struct ProjectResp {
    1: required Status status
    2: optional core.RawHeader header
    3: optional list<core.RawRow> rows
}

struct InsertReq {
    1: required string storageUnit
    2: required core.InsertData rawData;
}

struct DeleteReq {
    1: required string storageUnit
    2: optional core.RawFilter filter
    3: required list<string> patterns
    4: optional core.RawTagFilter tagFilter
}

struct GetStorageBoundaryResp {
    1: required Status status
    2: optional string startColumn
    3: optional string endColumn
}

struct StorageUnit {
    1: optional string name
}

struct GetColumnsOfStorageUnitResp {
    1: required Status status
    2: optional map<StorageUnit,set<core.RawField>> schemas
}

// TODO: throw exception directly, do not return Status
// TODO: replace req with more specific args
service FileStoreService {

    ProjectResp executeProject(1: ProjectReq req);

    Status executeInsert(1: InsertReq req);

    Status executeDelete(1: DeleteReq req);

    GetColumnsOfStorageUnitResp getColumnsOfStorageUnit();

    GetStorageBoundaryResp getBoundaryOfStorage(1: string prefix);

}