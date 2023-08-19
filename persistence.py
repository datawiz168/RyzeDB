import pickle
from table import MemTable

class Persistence:
    def __init__(self, log_file_path: str):
        self.log_file_path = log_file_path

    def persist_memtable(self, memtable: MemTable):
        # 将MemTable序列化并写入WAL（写前日志）
        with open(self.log_file_path, 'ab') as file:
            serialized_data = pickle.dumps(memtable)
            file.write(serialized_data)

    def recover_memtable(self) -> MemTable:
        # 从WAL（写前日志）恢复MemTable
        with open(self.log_file_path, 'rb') as file:
            serialized_data = file.read()
            memtable = pickle.loads(serialized_data)
            return memtable
