import shutil  # 导入shutil库，用于文件操作，如复制文件内容
import gzip   # 导入gzip库，用于处理gzip压缩文件
from typing import List  # 导入typing库的List类型，用于类型注解

class CompactionStrategy:
    def __init__(self, merge_threshold: int, compression_level: int):
        """
        初始化合并和压缩策略。
        :param merge_threshold: 合并阈值，当文件数量达到此阈值时触发合并。表示多少个SSTable文件将触发合并操作。
        :param compression_level: 压缩级别，用于确定压缩算法的强度。表示压缩操作的程度，不同的级别可能会影响压缩效率和压缩后文件的大小。
        """
        self.merge_threshold = merge_threshold
        self.compression_level = compression_level

    def should_merge(self, file_count: int) -> bool:
        """
        根据文件数量决定是否进行合并。
        :param file_count: 当前文件数量。表示当前SSTable文件的数量。
        :return: 如果应该合并，则返回True。当文件数量达到或超过合并阈值时，触发合并操作。
        """
        return file_count >= self.merge_threshold

    def merge_files(self, files: List[str]) -> str:
        """
        将给定的文件合并为一个文件。
        :param files: 要合并的文件列表。表示要合并的SSTable文件列表。
        :return: 合并后的文件路径。返回新的合并后的SSTable文件路径。
        """
        merged_file_path = "merged_file.txt"  # 定义合并后的文件路径
        with open(merged_file_path, 'w') as merged_file:  # 以写模式打开合并后的文件
            for file_path in files:  # 遍历要合并的文件列表
                with open(file_path, 'r') as file:  # 以读模式打开当前文件
                    shutil.copyfileobj(file, merged_file)  # 将当前文件的内容复制到合并后的文件
        return merged_file_path  # 返回合并后的文件路径

    def compress_file(self, file_path: str) -> str:
        """
        压缩给定的文件。
        :param file_path: 要压缩的文件路径。表示需要压缩的SSTable文件路径。
        :return: 压缩后的文件路径。返回压缩后的SSTable文件路径。
        """
        compressed_file_path = file_path + ".gz"  # 定义压缩后的文件路径
        with open(file_path, 'rb') as file_in:  # 以二进制读模式打开原文件
            with gzip.open(compressed_file_path, 'wb', compresslevel=self.compression_level) as file_out:  # 以二进制写模式打开压缩文件
                shutil.copyfileobj(file_in, file_out)  # 将原文件内容复制到压缩文件
        return compressed_file_path  # 返回压缩后的文件路径
