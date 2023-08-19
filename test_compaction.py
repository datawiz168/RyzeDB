import unittest  # 导入unittest库，用于创建和运行测试用例
from compaction import CompactionStrategy  # 从compaction模块导入CompactionStrategy类，用于测试
import os  # 导入os库，用于文件路径和文件操作


class TestCompactionStrategy(unittest.TestCase):

    def setUp(self):
        # 在测试开始前，创建一些测试文件
        # 这些文件将用于合并和压缩测试
        self.test_files = ["file1.txt", "file2.txt", "file3.txt"]
        for file_name in self.test_files:
            with open(file_name, 'w') as file:
                file.write(file_name)

        # 创建合并和压缩策略实例
        # 用于测试文件合并和压缩功能
        self.compaction_strategy = CompactionStrategy(merge_threshold=2, compression_level=5)

    def test_should_merge(self):
        # 测试合并阈值是否正常工作
        # 测试当文件数量低于阈值和高于阈值时的返回值
        self.assertFalse(self.compaction_strategy.should_merge(file_count=1))
        self.assertTrue(self.compaction_strategy.should_merge(file_count=3))

    def test_merge_files(self):
        # 测试文件合并
        # 验证合并后的文件是否存在
        merged_file = self.compaction_strategy.merge_files(self.test_files)
        self.assertTrue(os.path.exists(merged_file))

    def test_compress_file(self):
        # 测试文件压缩
        # 验证压缩后的文件是否存在
        compressed_file = self.compaction_strategy.compress_file(self.test_files[0])
        self.assertTrue(os.path.exists(compressed_file))

    def tearDown(self):
        # 在测试结束后，删除创建的测试文件
        # 清理测试环境
        for file_name in self.test_files + ["merged_file.txt", "file1.txt.gz"]:
            if os.path.exists(file_name):
                os.remove(file_name)


if __name__ == "__main__":
    # 如果直接运行此脚本，则运行测试
    unittest.main()
