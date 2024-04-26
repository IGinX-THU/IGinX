# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

from .thrift.rpc.ttypes import StorageEngineType
class ClusterInfo(object):

    def __init__(self, resp):
        self.__iginx_list = resp.iginxInfos
        self.__storage_engine_list = StorageEngineInfosWrapper(resp.storageEngineInfos)
        self.__meta_storage_list = resp.metaStorageInfos
        self.__local_meta_storage = resp.localMetaStorageInfo


    def get_iginx_list(self):
        return self.__iginx_list


    def get_storage_engine_list(self):
        return self.__storage_engine_list


    def get_local_meta_storage(self):
        return self.__local_meta_storage


    def get_meta_storage_list(self):
        return self.__meta_storage_list


    def is_use_local_meta_storage(self):
        return self.__local_meta_storage is not None


    def __str__(self):
        value = str(self.__iginx_list) + "\n" + str(self.__storage_engine_list) + "\n"
        if self.__meta_storage_list:
            value += str(self.__meta_storage_list)
        else:
            value += str(self.__local_meta_storage)
        return value


class StorageEngineInfosWrapper:
    def __init__(self, storage_engine_infos):
        self.storage_engine_infos = storage_engine_infos

    def __repr__(self):
        res = []
        for sei in self.storage_engine_infos:
            L = ['%s=%r' % (key, value if key != "type" else StorageEngineType._VALUES_TO_NAMES.get(int(value)))
                 for key, value in sei.__dict__.items()]
            res.append('%s(%s)' % (sei.__class__.__name__, ', '.join(L)))
        return '[%s]' % (', '.join(res))
