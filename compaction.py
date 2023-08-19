# 导入snappy库，用于压缩和解压缩数据
import snappy
# 导入类型提示库，以便在函数签名中使用类型提示
from typing import List

# 创建CompactionStrategy类，用于管理文件的合并和压缩
class CompactionStrategy:
    # 构造函数，初始化合并阈值
    def __init__(self, merge_threshold: int):
        """
        初始化合并策略。
        :param merge_threshold: 合并阈值，当文件数量达到此阈值时触发合并。
        """
        self.merge_threshold = merge_threshold

    # 判断是否应该合并文件
    def should_merge(self, file_count: int) -> bool:
        """
        根据文件数量决定是否进行合并。
        :param file_count: 当前文件数量。
        :return: 如果应该合并，则返回True。
        """
        return file_count >= self.merge_threshold

    # 合并文件并压缩
    def merge_files(self, files: List[str]) -> str:
        """
        将给定的文件合并为一个文件。
        :param files: 要合并的文件列表。
        :return: 合并后的文件路径。
        """
        merged_file_path = "merged_file.txt"
        with open(merged_file_path, 'wb') as merged_file:
            for file_path in files:
                with open(file_path, 'rb') as file:
                    content = file.read()
                    compressed_content = snappy.compress(content)
                    # 写入压缩内容的长度和压缩内容
                    merged_file.write(len(compressed_content).to_bytes(4, 'big'))
                    merged_file.write(compressed_content)
        return merged_file_path

    # 解压缩文件
    def decompress_file(self, file_path: str) -> bytes:
        """
        解压缩给定的文件，并使用Snappy算法进行解压缩。
        :param file_path: 要解压缩的文件路径。
        :return: 解压缩后的内容。
        """
        # 初始化解压后的内容
        decompressed_content = b""
        # 打开文件并读取内容
        with open(file_path, 'rb') as file_in:
            while True:
                # 读取4个字节的长度信息
                length_bytes = file_in.read(4)
                if not length_bytes:
                    break
                # 将长度信息从字节转换为整数
                length = int.from_bytes(length_bytes, 'big')
                # 读取压缩内容
                compressed_content = file_in.read(length)
                # 解压缩内容并添加到解压后的内容中
                decompressed_content += snappy.decompress(compressed_content)
        return decompressed_content
