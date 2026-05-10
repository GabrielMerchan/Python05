#!/usr/bin/env python3

from typing import Any
import abc


class DataProcessor(abc.ABC):
    _data: list[tuple[int, str]]

    @abc.abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abc.abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        return self._data.pop(0)


class NumericProcessor(DataProcessor):
    def __init__(self):
        self._data = []
        self._counter = 0

    def validate(self, data: int | float | list[int] |
                 list[float] | list[int | float]) -> bool:
        return (isinstance(data, (int, float)) or
                (isinstance(data, list) and
                 all(isinstance(x, (int, float)) for x in data)))

    def ingest(self, data: int | float | list[int] |
               list[float] | list[int | float]) -> None:
        try:
            if self.validate(data) is False:
                raise ValueError("Got exception: Not proper data type")
        except ValueError as e:
            print(f"{e}")
            return
        if isinstance(data, (int, float)):
            self._data.append((self._counter, str(data)))
            self._counter += 1
        else:
            for x in data:
                self._data.append((self._counter, str(x)))
                self._counter += 1


class TextProcessor(DataProcessor):
    def __init__(self):
        self._data = []
        self._counter = 0

    def validate(self, data: str | list[str]) -> bool:
        return isinstance(data, str) or (isinstance(data, list) and
                                         all(isinstance(x, str)for x in data))

    def ingest(self, data: str | list[str]) -> None:
        try:
            if self.validate(data) is False:
                raise ValueError("Got exception: Not proper data type")
        except ValueError as e:
            print(f"{e}")
            return
        if isinstance(data, str):
            self._data.append((self._counter, data))
            self._counter += 1
        else:
            for x in data:
                self._data.append((self._counter, x))
                self._counter += 1


class LogProcessor(DataProcessor):
    def __init__(self):
        self._data = []
        self._counter = 0

    def validate(self, data: dict[str, str] | list[dict[str, str]]) -> bool:
        return (isinstance(data, dict)
                and all(isinstance(k, str) and isinstance(v, str)
                        for k, v in data.items())
                or (isinstance(data, list)
                and all(isinstance(x, dict) and
                        all(isinstance(k, str) and
                            isinstance(v, str)
                            for k, v in x.items())for x in data)))

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        try:
            if self.validate(data) is False:
                raise ValueError("Got exception: Not proper data type")
        except ValueError as e:
            print(f"{e}")
            return
        if isinstance(data, dict):
            values = list(data.values())
            message = f"{values[0]}: {values[1]}"
            self._data.append((self._counter, message))
            self._counter += 1
        else:
            for x in data:
                values = list(x.values())
                message = f"{values[0]}: {values[1]}"
                self._data.append((self._counter, message))
                self._counter += 1


if __name__ == "__main__":
    processor = LogProcessor()

    # # Caso 1: string
    # processor.ingest(1)
    # result = processor.output()

    # print("Output:")
    # print(result)
    # # Caso 2: lista de strings

    # # Caso inválido (descomenta para probar el error)
    # processor.ingest([1, 'a', 3])

    # result = processor.output()

    processor.ingest([{'log_level': 'NOTICE',
                      'log_mesage': 'Connection to server'}, {'log_level': 'ERROR',
                      'log_mesage': 'Unauthorized access!!'}])
    result1 = processor.output()
    print("Output:")
    print(f"Log entry {result1[0]}: {result1[1]}")
    result2 = processor.output()
    print("Output:")
    print(f"Log entry {result2[0]}: {result2[1]}")
    # result2 = processor.output()
    # print("Output:")
    # print(result2)
    # result3 = processor.output()
    # print("Output:")
    # print(result3)
