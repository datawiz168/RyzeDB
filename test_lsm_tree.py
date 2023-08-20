import unittest
from lsm_tree import LSMT

class TestLSMT(unittest.TestCase):
    def setUp(self):
        self.lsm_tree = LSMT()

    def test_put_and_get(self):
        # 测试插入和获取键值对
        self.lsm_tree.put('key1', 'value1')
        self.assertEqual(self.lsm_tree.get('key1'), 'value1')

    def test_delete(self):
        # 测试删除键值对
        self.lsm_tree.put('key2', 'value2')
        self.lsm_tree.delete('key2')
        self.assertIsNone(self.lsm_tree.get('key2'))

    def test_compact(self):
        # 测试压缩功能
        for i in range(1001): # 假设压缩阈值为1000
            self.lsm_tree.put(f'key{i}', f'value{i}')
        # 这里可以添加检查以确保压缩已发生

    def test_persistence_and_recovery(self):
        # 测试持久化和恢复功能
        self.lsm_tree.put('key3', 'value3')
        self.lsm_tree.compact()
        recovered_lsm_tree = LSMT()
        recovered_lsm_tree.recover()
        self.assertEqual(recovered_lsm_tree.get('key3'), 'value3')

    def test_edge_cases(self):
        # 测试边缘情况，例如插入空键或空值
        self.lsm_tree.put('', 'empty_key')
        self.lsm_tree.put('key4', '')
        self.assertEqual(self.lsm_tree.get(''), 'empty_key')
        self.assertEqual(self.lsm_tree.get('key4'), '')

if __name__ == "__main__":
    unittest.main()
