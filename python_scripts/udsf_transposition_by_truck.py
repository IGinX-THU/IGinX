KEY = "key"

TYPE_BOOLEAN = 1
TYPE_INTEGER = 2
TYPE_LONG = 3
TYPE_FLOAT = 4
TYPE_DOUBLE = 5
TYPE_BINARY = 6

class Field:
    def __init__(self, path, type):
        self.path = path
        self.type = type
        self.truck = self.path.split('.')[1].encode()
        self.name = self.path.split('.')[-1].encode()

    def get_truck(self):
        return self.truck

    def get_name(self):
        return self.name


class UDFTranspositionByTruck:
    def __init__(self):
        pass

    def transform(self, rows):
        key = None
        fields = []

        for i in range(len(rows[0])):
            path = rows[0][i]
            type = rows[1][i]
            if i == 0 and path == KEY:
                key = KEY
                fields.append(None)
                continue
            fields.append(Field(path, type))

        rets = []
        paths = []
        types = []

        if key:
            paths.append(KEY)
            types.append(TYPE_LONG)

        paths.append('truck')
        paths.append('name')
        paths.append('value')

        types.append(TYPE_BINARY)
        types.append(TYPE_BINARY)
        types.append(TYPE_DOUBLE)

        rets.append(paths)
        rets.append(types)

        for row in rows[2:]:
            ts = None
            if key:
                ts = row[0]

            for i in range(len(row)):
                if i == 0 and key:
                    continue

                values = []
                if key:
                    values.append(ts)
                field = fields[i]
                values.append(field.get_truck())
                values.append(field.get_name())
                values.append(row[i])
                rets.append(values)

        return rets



