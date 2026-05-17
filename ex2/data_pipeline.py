#!/usr/bin/env python3

from typing import Any, Protocol
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


class ExportPlugin(Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        pass


class DataStream():
    def __init__(self):
        self._processors = []
        self._counters = [0, 0, 0]

    def register_processor(self, proc: DataProcessor) -> None:
        self._processors.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        try:
            if len(self._processors) == 0 or len(stream) == 0:
                raise ValueError("No processor found, no data")
            for i in stream:
                found = False
                for p in self._processors:
                    if p.validate(i):
                        p.ingest(i)
                        found = True
                        break
                if found is False:
                    print("DataStream error - Can't process"
                          f" element in stream: {i}")
        except ValueError as e:
            print(f"{e}")

    def print_processors_stats(self) -> None:
        for i in self._processors:
            if i.__class__.__name__ == "NumericProcessor":
                self._counters[0] = i._counter
                print(f"Numeric Processor: total {self._counters[0]} items"
                      " processed, remaining {len(i._data)} on processor")
            if i.__class__.__name__ == "TextProcessor":
                self._counters[1] = i._counter
                print(f"Text Processor: total {self._counters[1]} items"
                      " processed, remaining {len(i._data)} on processor")
            if i.__class__.__name__ == "LogProcessor":
                self._counters[2] = i._counter
                print(f"Log Processor: total {self._counters[2]} items"
                      " processed, remaining {len(i._data)} on processor")

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        pass


if __name__ == "__main__":
    lista = ['Hello world', [3.14, -1, 2.71],
             [{'log_level': 'WARNING', 'log_message': 'Telnet '
               'access! Use ssh instead'},
              {'log_level': 'INFO', 'log_message': 'User wil isconnected'}],
             42, ['Hi', 'five']]

    processor = DataStream()
    processor.process_stream(lista)
    print("\nRegistering Numeric Processor\n")
    num_proc = NumericProcessor()
    processor.register_processor(num_proc)
    processor.process_stream(lista)
    print("== DataStream statistics ==")
    processor.print_processors_stats()
    print("\nRegistering other data processors")
    txt_proc = TextProcessor()
    log_proc = LogProcessor()
    processor.register_processor(txt_proc)
    processor.register_processor(log_proc)
    print("Send the same batch again")
    processor.process_stream(lista)
    print("== DataStream statistics ==")
    processor.print_processors_stats()

    print("\nConsume some elements from the data"
          " processors: Numeric 3, Text 2, Log 1")
    num_proc.output()
    num_proc.output()
    num_proc.output()
    txt_proc.output()
    txt_proc.output()
    log_proc.output()
    print("== DataStream statistics ==")
    processor.print_processors_stats()
