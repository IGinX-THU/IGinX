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
