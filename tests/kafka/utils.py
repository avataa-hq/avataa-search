from typing import Union


class KafkaMSGMock:
    def __init__(
        self, msg_key: str, msg_topic: str, msg_value: Union[bytes, dict]
    ):
        self.msg_key = msg_key
        self.msg_topic = msg_topic
        self.msg_value = msg_value

    def topic(self):
        return self.msg_topic

    def value(self):
        return self.msg_value

    def key(self):
        return self.msg_key.encode("utf-8")


def function_call_count_decorator(func):
    async def funct_counter(*args, **kwargs):
        funct_counter.call_count += 1
        return await func(*args, **kwargs)

    funct_counter.call_count = 0

    return funct_counter


@function_call_count_decorator
async def async_mock_func_do_nothing(*args, **kwargs):
    # self.call_count
    pass
