# 导入unittest库，用于编写和运行测试
import unittest
# 从持久化模块导入PersistenceManager类
from persistence import PersistenceManager
# 从数据模型模块导入KeyValue类
from data_model import KeyValue
# 从表模块导入MemTable类
from table import MemTable
# 导入os库，用于文件操作，如删除WAL文件
import os


# 定义测试持久化的测试类
class TestPersistence(unittest.TestCase):
    def setUp(self):
        """在每个测试开始前执行的方法。"""
        # 创建持久化对象，WAL文件路径为'test_wal.log'
        self.persistence = PersistenceManager('test_wal.log')
        # 创建内存表对象
        self.memtable = MemTable()
        # 向内存表中插入两个键值对
        self.memtable.put('key1', KeyValue('key1', 'value1'))
        self.memtable.put('key2', KeyValue('key2', 'value2'))

    def tearDown(self):
        """每个测试结束后执行的方法。"""
        # 如果WAL文件存在，则删除它
        if os.path.exists('test_wal.log'):
            os.remove('test_wal.log')

    def test_append_to_wal(self):
        """测试将操作及其关联数据追加到WAL。"""
        # 定义要测试的操作和数据
        operation = 'insert'
        data = [KeyValue(k, v) for k, v in self.memtable.data.items()]
        # 使用持久化对象将操作和数据追加到WAL
        self.persistence.append_to_wal(operation, data)

        # 从持久化对象中加载WAL内容
        wal_content = self.persistence.load_wal()
        # 验证WAL内容的长度是否为1
        self.assertEqual(len(wal_content), 1)
        # 验证WAL内容的第一个元素的操作是否与预期相符
        self.assertEqual(wal_content[0][0], operation)
        # 验证WAL内容的第一个元素的数据是否与预期相符
        self.assertEqual(wal_content[0][1], data)

    # ... 其他测试方法，您可以按照上述模式继续添加 ...


# 如果此文件作为主程序运行，则运行所有测试
if __name__ == "__main__":
    unittest.main()


