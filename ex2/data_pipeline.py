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

    def get_storage_l(self) -> list[str]:
        return self._storage


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


class DataStream():
    def __init__(self) -> None:
        self.actualproc: dict[DataProcessor, int] = {}

    def register_processor(self, proc: DataProcessor) -> None:
        self.actualproc.update({proc: 0})

    def process_stream(self, stream: list[typing.Any]) -> None:
        procs = self.actualproc
        for element in stream:
            anyvalid = False
            for valid in procs.keys():
                if valid.validate(element):
                    valid.ingest(element)
                    if isinstance(element, list):
                        procs[valid] += len(element)
                    else:
                        procs[valid] += 1
                    anyvalid = True
            if anyvalid is not True:
                print('DataStream error - Can’t '
                      f'process element in stream: {element}')

    def print_processors_stats(self) -> None:
        i = 1
        if len(self.actualproc.keys()):
            for procs in self.actualproc.keys():
                print(f'Processor {i}: total {self.actualproc[procs]} '
                      'items processed, remaining'
                      f' {len(procs.get_storage_l())}'
                      ' on processor')
                i += 1
        else:
            print('No processor found, no data')

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for proc in self.actualproc.keys():
            thel = []
            for i in range(nb):
                try:
                    thel.append(proc.output())
                except IndexError:
                    break
            if len(thel):
                plugin.process_output(thel)
            else:
                print('This proc has no elements')


class ExportPlugin(typing.Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        pass


class CSVPlugin():
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print('CSV Output:')
        print(data[0][1], end='')
        for element in data[1:]:
            print(f',{element[1]}', end='')
        print()


class JSONPlugin():
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print('JSON Output:')
        print('{"item_' + str(data[0][0]) + '": "' + data[0][1], end='"')
        for rank, element in data[1:]:
            print(f', "item_{rank}": "{element}"', end='')
        print('}')


if __name__ == "__main__":
    print('=== Code Nexus - Data Pipeline ===\n')
    print('Initialize Data Stream...\n')
    datastrm = DataStream()
    print('== DataStream statistics ==')
    datastrm.print_processors_stats()
    datastrm.register_processor(NumericProcessor())
    datastrm.register_processor(TextProcessor())
    datastrm.register_processor(LogProcessor())
    print('\nRegistering Processors\n')
    data = ['Hello world', [3.14, -1, 2.71], [
            {'log_level': 'WARNING',
             'log_message': 'Telnet access! Use ssh instead'},
            {'log_level': 'INFO', 'log_message': 'User wil is connected'}],
            42, ['Hi', 'five']]
    print(f'Send first batch of data on stream: {data}\n')
    datastrm.process_stream(data)
    print('== DataStream statistics ==')
    datastrm.print_processors_stats()
    print('\nSend 3 processed data from each processor to a CSV plugin:')
    datastrm.output_pipeline(3, CSVPlugin())
    print('\n== DataStream statistics ==')
    datastrm.print_processors_stats()
    data = [21, ['I love AI', 'LLMs are wonderful', 'Stay healthy'],
            [{'log_level': 'ERROR', 'log_message': '500 server crash'},
            {'log_level': 'NOTICE',
             'log_message': 'Certificate expires in 10 days'}],
            [32, 42, 64, 84, 128, 168], 'World hello']
    print(f'\nSend another batch of data: {data}')
    datastrm.process_stream(data)
    print('\n== DataStream statistics ==')
    datastrm.print_processors_stats()
    print('\nSend 5 processed data from each processor to a JSON plugin:')
    datastrm.output_pipeline(5, JSONPlugin())
    print('\n== DataStream statistics ==')
    datastrm.print_processors_stats()
