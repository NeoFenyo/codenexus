import abc
import typing


class DataProcessor(abc.ABC):
    def __init__(self) -> None:
        self._storage: list[str] = []
        self._rank = 0

    @abc.abstractmethod
    def validate(self, data: typing.Any) -> bool:
        pass

    @abc.abstractmethod
    def ingest(self, data: typing.Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        temp = self.get_storage_i(0)
        self.storage_pop(0)
        arank = self.get_rank()
        self.rank_i()
        return arank, temp

    def append_storage(self, new: str) -> None:
        self._storage.append(new)

    def rank_i(self) -> None:
        self._rank += 1

    def get_rank(self) -> int:
        return self._rank

    def storage_pop(self, index: int) -> None:
        self._storage.pop(index)

    def get_storage_i(self, index: int) -> str:
        return self._storage[index]


class NumericProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()

    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, list) and len(data):
            for n in data:
                if isinstance(n, (int, float)) is not True:
                    return False
            return True
        else:
            return isinstance(data, (int, float))

    def ingest(self, data: int | float | list[int] | list[float]) -> None:
        if self.validate(data):
            if isinstance(data, list):
                for n in data:
                    super().append_storage(str(n))
            else:
                super().append_storage(str(data))
        else:
            raise TypeError("Improper numeric data")


class TextProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()

    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, list) and len(data):
            for n in data:
                if isinstance(n, (str)) is not True:
                    return False
            return True
        else:
            return isinstance(data, (str))

    def ingest(self, data: str | list[str]) -> None:
        if self.validate(data):
            if isinstance(data, list):
                for n in data:
                    super().append_storage(n)
            else:
                super().append_storage(data)
        else:
            raise TypeError("Improper text data")


class LogProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()

    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, list):
            for dic in data:
                if isinstance(dic, dict) and len(dic):
                    for k, v in dic.items():
                        if isinstance(v, str) is not True:
                            return False
                        if isinstance(k, str) is not True:
                            return False
                else:
                    return False
            return True
        elif isinstance(data, dict) and len(data):
            for k, v in data.items():
                if isinstance(v, str) is not True:
                    return False
                if isinstance(k, str) is not True:
                    return False
            return True
        else:
            return False

    def ingest(self, data: list[dict[str, str]] | dict[str, str]) -> None:
        if self.validate(data):
            if isinstance(data, dict):
                n = ": ".join(data.values())
                super().append_storage(n)
            else:
                for dic in data:
                    n = ": ".join(dic.values())
                    super().append_storage(n)
        else:
            raise TypeError("Improper log data")


if __name__ == "__main__":
    print("=== Code Nexus - Data Processor ===\n")
    print('Testing Numeric Processor...')
    nprocess = NumericProcessor()
    print(f"Trying to validate input '42': {nprocess.validate(42)}")
    print(f"Trying to validate input 'Hello': {nprocess.validate("Hello")}")
    print("Test invalid ingestion of string ’foo’"
          "without prior validation: ", end='')
    try:
        nprocess.ingest('foo')
    except TypeError as e:
        print(f'Got exception: {e}')
    datai = [1, 2, 3, 4, 5]
    print(f'Processing data: {datai}')
    nprocess.ingest(datai)
    print('Extracting 3 values...')
    for i in range(3):
        res = nprocess.output()
        print(f'Numeric value {res[0]}: {res[1]}')
    print('\nTesting Text Processor...')
    tprocess = TextProcessor()
    print(f"Trying to validate input '42': {tprocess.validate(42)}")
    datat = ['Hello', 'Nexus', 'World']
    print(f'Processing data: {datat}')
    tprocess.ingest(datat)
    print('Extracting 1 value...')
    res = tprocess.output()
    print(f'Text value {res[0]}: {res[1]}')
    print('\nTesting Log Processor...')
    lprocess = LogProcessor()
    print(f"Trying to validate input 'Hello': {lprocess.validate("Hello")}")
    data = [{'log_level': 'NOTICE', 'log_message': 'Connection to server'},
            {'log_level': 'ERROR', 'log_message': 'Unauthorized access!!'}]
    lprocess.ingest(data)
    print('Extracting 2 value...')
    for i in range(2):
        res = lprocess.output()
        print(f'Log entry {res[0]}: {res[1]}')
