from typing import Protocol


class ProtocolKafkaMSGCounter(Protocol):
    def __init__(self):
        self.__number_of_empty_msg = 0

    @property
    def number_of_empty_messages(self):
        return self.__number_of_empty_msg

    def plus_one(self):
        self.__number_of_empty_msg += 1

    def refresh(self):
        self.__number_of_empty_msg = 0
