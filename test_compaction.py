# 导入unittest库，用于编写和运行测试用例
import unittest
# 导入os库，用于文件操作，如删除文件
import os
# 导入要测试的类CompactionStrategy
from compaction import CompactionStrategy

# 创建测试CompactionStrategy类的测试类
class TestCompactionStrategy(unittest.TestCase):
    # 在每个测试用例运行之前执行，用于设置测试环境
    def setUp(self):
        # 要合并的文件列表
        self.files_to_merge = ["file1.txt", "file2.txt"]
        # 创建这些文件并写入内容
        for file_path in self.files_to_merge:
            with open(file_path, 'wb') as file:
                file.write(file_path.encode('utf-8'))
        # 创建一个合并阈值为2的CompactionStrategy对象
        self.compaction_strategy = CompactionStrategy(merge_threshold=2)

    # 在每个测试用例运行之后执行，用于清理测试环境
    def tearDown(self):
        # 删除创建的文件
        for file_path in self.files_to_merge:
            os.remove(file_path)
        os.remove("merged_file.txt")

    # 测试文件合并功能
    def test_merge_files(self):
        # 调用合并文件方法
        merged_file_path = self.compaction_strategy.merge_files(self.files_to_merge)
        # 获取原始文件内容
        original_content = b""
        for file_path in self.files_to_merge:
            with open(file_path, 'rb') as file:
                original_content += file.read()
        # 获取合并后文件的内容
        compressed_content = self.compaction_strategy.decompress_file(merged_file_path)
        # 断言合并后的内容与原始内容相同
        self.assertEqual(compressed_content, original_content)

# 如果直接运行此脚本，则执行测试
if __name__ == "__main__":
    unittest.main()
