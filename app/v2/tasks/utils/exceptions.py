class SearchException(Exception):
    pass


class SearchPermissionException(SearchException):
    pass


class SearchValueError(SearchException):
    pass


class SearchNotFoundException(SearchException):
    pass


class ElementFound(BaseException):
    pass


class ElementNotFound(BaseException):
    pass
