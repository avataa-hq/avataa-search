from services.base_single_tone.utils import SingletonMeta


class KafkaMSGCounter(metaclass=SingletonMeta):
    def __init__(self):
        self.__number_of_empty_msg = 0

    @property
    def number_of_empty_messages(self):
        return self.__number_of_empty_msg

    def plus_one(self):
        self.__number_of_empty_msg += 1

    def refresh(self):
        self.__number_of_empty_msg = 0
