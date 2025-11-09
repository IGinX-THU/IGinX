from abc import ABC, abstractmethod

"""
Base UDF Wrapper Class
__cls: user's UDF class
_wrapped: instance of user's UDF class
"""
class UDFWrapper(ABC):
    def __init__(self, cls):
        self.__cls = cls

    def __call__(self, *args, **kwargs):
        self._wrapped = self.__cls(*args, **kwargs)
        return self

    def __getattr__(self, item):
        return getattr(self._wrapped, item)

    @abstractmethod
    def transform(self, data, *args, **kwargs):
        """
        called by Java
        """

