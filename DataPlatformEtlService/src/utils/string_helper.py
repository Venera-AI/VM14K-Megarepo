import re


class StringHelperUtils:
    @staticmethod
    def is_valid_uuid(val: str):
        regex = re.compile(
            "^[a-f0-9]{8}-?[a-f0-9]{4}-?4[a-f0-9]{3}-?[89ab][a-f0-9]{3}-?[a-f0-9]{12}\Z",
            re.I,
        )
        return bool(regex.match(val))

    @staticmethod
    def is_valid_digit_string(val: str):
        return val.isdigit()
