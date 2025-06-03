class BaseDataTypeMapper:
    def map_type(self, column):
        raise NotImplementedError("Subclasses must implement this method")
