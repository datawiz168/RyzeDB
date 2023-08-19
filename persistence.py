# 导入pickle库，用于序列化和反序列化Python对象
import pickle
# 导入os库，用于操作文件和目录
import os
# 从表模块导入MemTable类
from table import MemTable
# 从数据模型模块导入KeyValue类
from data_model import KeyValue

# 定义持久化管理器类
class PersistenceManager:
    def __init__(self, wal_file: str = "wal.log"):
        """初始化持久化管理器，设置WAL文件路径。"""
        self.wal_file = wal_file
        self._init_wal_file()

    def _init_wal_file(self):
        """创建或打开WAL文件。如果文件不存在，则创建一个空文件。"""
        if not os.path.exists(self.wal_file):
            with open(self.wal_file, 'wb') as file:
                pass

    def append_to_wal(self, operation: str, data: [KeyValue]):
        """将操作及其关联数据追加到WAL。以二进制追加模式打开文件，并将条目序列化后追加到文件中。"""
        with open(self.wal_file, 'ab') as file:
            entry = (operation, data)
            pickle.dump(entry, file)

    def load_wal(self) -> [(str, [KeyValue])]:
        """从WAL加载所有操作和数据。通过反序列化文件内容来加载所有条目，并返回操作列表。"""
        operations = []
        with open(self.wal_file, 'rb') as file:
            try:
                while True:
                    entry = pickle.load(file)
                    operations.append(entry)
            except EOFError:
                pass
        return operations

    def clear_wal(self):
        """清空WAL文件。以二进制写入模式打开文件，并立即关闭，从而清空文件内容。"""
        with open(self.wal_file, 'wb') as file:
            pass

