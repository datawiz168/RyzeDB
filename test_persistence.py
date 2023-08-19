import unittest  # 导入unittest库，用于单元测试
from persistence import Persistence  # 导入Persistence类
from table import MemTable  # 导入MemTable类

class TestPersistence(unittest.TestCase):  # 定义测试类
    def setUp(self):  # 测试前的设置
        self.memtable = MemTable()  # 创建MemTable对象
        self.memtable.put('key1', 'value1')  # 向MemTable中添加数据
        self.persistence = Persistence('test_log_file.log')  # 创建Persistence对象

    def tearDown(self):  # 测试后的清理
        import os
        os.remove('test_log_file.log')  # 删除测试日志文件

    def test_persist_and_recover_memtable(self):  # 测试持久化和恢复MemTable
        self.persistence.persist_memtable(self.memtable)  # 持久化MemTable
        recovered_memtable = self.persistence.recover_memtable()  # 恢复MemTable
        self.assertEqual(recovered_memtable.get('key1'), 'value1')  # 验证恢复的数据
