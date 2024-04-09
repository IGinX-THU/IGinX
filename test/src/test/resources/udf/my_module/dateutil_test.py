from dateutil import parser


class Test:
    def transform(self, data, args, kvargs):
        datetime_string = "2023-04-05 14:30:00"
        parsed_datetime = parser.parse(datetime_string)

        # 验证解析结果
        if parsed_datetime.year == 2023 and \
                parsed_datetime.month == 4 and \
                parsed_datetime.day == 5 and \
                parsed_datetime.hour == 14 and \
                parsed_datetime.minute == 30:
            return [
                ["year", "month", "day", "hour", "minute"],
                ["LONG", "LONG", "LONG", "LONG", "LONG"],
                [parsed_datetime.year, parsed_datetime.month, parsed_datetime.day, parsed_datetime.hour, parsed_datetime.minute]
            ]
        else:
            return []

