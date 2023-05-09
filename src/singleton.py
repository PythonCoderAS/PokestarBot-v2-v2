from typing import Self, Optional


class SingletonClass:
    __instance: Optional[Self] = None

    def __new__(cls, *args, **kwargs) -> Self:
        if cls.__instance is None:
            cls.__instance = super().__new__(cls)
        return cls.__instance
