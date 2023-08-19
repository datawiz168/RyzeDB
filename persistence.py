import pickle  # 导入pickle库，用于对象的序列化和反序列化
from table import MemTable  # 导入MemTable类

class Persistence:
    def __init__(self, log_file_path: str):
        self.log_file_path = log_file_path  # 设置日志文件的路径

    def persist_memtable(self, memtable: MemTable):
        # 序列化MemTable的key-value对并写入WAL（写前日志）
        with open(self.log_file_path, 'wb') as file:  # 以二进制写入模式打开文件
            serialized_data = pickle.dumps(memtable.data)  # 将MemTable的数据序列化
            file.write(serialized_data)  # 将序列化后的数据写入文件

    def recover_memtable(self) -> MemTable:
        # 从WAL恢复key-value对并重构MemTable
        with open(self.log_file_path, 'rb') as file:  # 以二进制读取模式打开文件
            serialized_data = file.read()  # 读取序列化的数据
            data = pickle.loads(serialized_data)  # 反序列化数据
            memtable = MemTable()  # 创建MemTable对象
            memtable.data = data  # 将反序列化的数据赋给MemTable
            return memtable  # 返回重构后的MemTable
