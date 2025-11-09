from .udf_base import UDFWrapper

class UDSFWrapper(UDFWrapper):
    def transform(self, data, *args, **kwargs):
        return self._wrapped.eval(data, *args, **kwargs)
