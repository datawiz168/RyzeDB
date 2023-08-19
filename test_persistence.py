import unittest
from table import MemTable
from persistence import Persistence

class TestPersistence(unittest.TestCase):
    def setUp(self):
        self.log_file_path = 'wal.log'
        self.memtable = MemTable()
        self.memtable.set('key1', 'value1')
        self.memtable.set('key2', 'value2')
        self.persistence = Persistence(self.log_file_path)

    def test_persist_and_recover_memtable(self):
        # 测试将MemTable持久化到WAL
        self.persistence.persist_memtable(self.memtable)

        # 从WAL中恢复MemTable
        recovered_memtable = self.persistence.recover_memtable()

        # 验证恢复后的MemTable与原始MemTable相同
        self.assertEqual(self.memtable.get('key1'), recovered_memtable.get('key1'))
        self.assertEqual(self.memtable.get('key2'), recovered_memtable.get('key2'))

    def tearDown(self):
        # 清理测试生成的文件
        import os
        os.remove(self.log_file_path)

if __name__ == '__main__':
    unittest.main()
