from typing import Optional


class CachingError(BaseException):
    def __init__(self, message, exc: Optional[BaseException] = None) -> None:
        super().__init__(message)
        self.exc = exc
