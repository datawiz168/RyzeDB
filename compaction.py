import snappy
from typing import List

class CompactionStrategy:
    def __init__(self, merge_threshold: int):
        """
        初始化合并策略。
        :param merge_threshold: 合并阈值，当文件数量达到此阈值时触发合并。
        """
        self.merge_threshold = merge_threshold

    def should_merge(self, file_count: int) -> bool:
        """
        根据文件数量决定是否进行合并。
        :param file_count: 当前文件数量。
        :return: 如果应该合并，则返回True。
        """
        return file_count >= self.merge_threshold

    def merge_files(self, files: List[str]) -> str:
        """
        将给定的文件合并为一个文件，并使用Snappy算法进行压缩。
        :param files: 要合并的文件列表。
        :return: 合并后的文件路径。
        """
        merged_file_path = "merged_file.txt"
        with open(merged_file_path, 'wb') as merged_file:
            for file_path in files:
                with open(file_path, 'rb') as file:
                    content = file.read()
                    compressed_content = snappy.compress(content)  # 使用Snappy算法压缩内容
                    merged_file.write(compressed_content)
        return merged_file_path

    def decompress_file(self, file_path: str) -> bytes:
        """
        解压缩给定的文件，并使用Snappy算法进行解压缩。
        :param file_path: 要解压缩的文件路径。
        :return: 解压缩后的内容。
        """
        decompressed_content = b""
        with open(file_path, 'rb') as file_in:
            compressed_content = file_in.read()
            decompressed_content = snappy.decompress(compressed_content)  # 使用Snappy算法解压缩内容
        return decompressed_content
