from data_model import KeyValue  # 从data_model模块导入KeyValue类，用于表示键值对
import json  # 导入JSON库，用于序列化和反序列化字典，以便在文件或网络传输中存储和恢复字典结构
import threading  # 导入线程库，用于同步操作，确保多线程环境下的数据一致性和线程安全
from typing import Optional  # 导入Optional类型，用于类型注解，表示一个类型可以是None
from typing import List  # 导入List类型，用于类型注解，表示列表类型
from typing import Dict  # 导入Dict类型，用于类型注解，表示字典类型


class MemTable:
    def __init__(self, threshold: int = 1000):
        self.data = {}  # 创建字典以存储键值对
        self.threshold = threshold  # 设置持久化阈值
        self.lock = threading.Lock()  # 创建锁，确保线程安全

    def put(self, key: str, value: KeyValue) -> None:
        with self.lock:  # 获取锁，确保线程安全
            self.data[key] = value  # 将键值对存储在字典中
            if len(self.data) >= self.threshold:  # 检查是否达到持久化阈值
                self.compact()  # 如果达到阈值，则持久化数据

    def get(self, key: str) -> Optional[KeyValue]:
        with self.lock:  # 获取锁，确保线程安全
            return self.data.get(key)  # 使用字典的get方法获取值

    def delete(self, key: str):
        with self.lock:  # 获取锁，确保线程安全
            if key in self.data:  # 检查键是否存在
                del self.data[key]  # 如果存在，则删除

    def compact(self):
        print("Starting compact...")  # 打印开始压缩的消息
        print("Opening file 'memtable.json' for writing...")  # 打印正在打开文件的消息
        with open('memtable.json', 'w') as file:  # 以写入模式打开名为'memtable.json'的文件，确保文件在操作完成后被关闭
            print("File opened successfully.")  # 打印文件成功打开的消息
            print("Dumping data to file...")  # 打印正在将数据转储到文件的消息
            # 使用列表推导式将MemTable中的键值对序列化为JSON对象
            serialized_data = {k: v.serialize() for k, v in self.data.items()}
            # 将序列化后的数据以JSON格式写入文件
            json.dump(serialized_data, file)
            print("Data dumped to file.")  # 打印数据已转储到文件的消息
        print("Clearing MemTable...")  # 打印正在清空MemTable的消息
        self.data.clear()  # 清空MemTable中的数据
        print("MemTable cleared.")  # 打印MemTable已清空的消息

    def recover(self, log_path: str):
        try:  # 尝试打开文件
            with open(log_path, 'r') as file:  # 打开文件以读取
                data = json.load(file)  # 使用json.load从文件加载字典
                # 使用键值对的deserialize方法从解析的数据中恢复对象
                self.data = {k: KeyValue.deserialize(v) for k, v in data.items()}
        except FileNotFoundError:  # 捕获文件未找到错误
            print(f"Log file {log_path} not found. Starting with an empty MemTable.")  # 打印错误消息

class SSTable:
    def __init__(self, filename):
       self.filename = filename
       self.file = open(filename, 'wb')  # 注意 'wb' 模式，以二进制写入
       self.index = {}  # 索引字典，用于存储键的位置和长度

    def write_from_memtable(self, memtable_data: Dict[str, KeyValue]):
    # 将memtable_data按键排序
        data = sorted(memtable_data.items(), key=lambda x: x[0])
        # 写入数据区域并创建索引
        for key, value in data:
            position = self.file.tell()  # 获取当前文件位置
            serialized_value = value.serialize()  # 使用KeyValue的serialize方法
            serialized_value_bytes = serialized_value.encode('utf-8')  # 将序列化的值转为字节串
            self.file.write(serialized_value_bytes)  # 写入序列化的值
            self.index[key] = (position, len(serialized_value_bytes))  # 添加索引
        # 写入索引区域
        index_position = self.file.tell()
        serialized_index = json.dumps(self.index)
        self.file.write(serialized_index.encode('utf-8'))
        # 写入元数据区域
        metadata = {
            'index_position': index_position,
            'index_length': len(serialized_index)
        }
        self.file.write(json.dumps(metadata).encode('utf-8'))
        self.file.close()  # 关闭文件

    def get(self, key: str) -> Optional[KeyValue]:
        # 打开文件
        self.file = open(self.filename, 'rb')

        if key in self.index:
            # 读取索引
            position, length = self.index[key]
            # 定位到文件中的正确位置
            self.file.seek(position)
            # 读取序列化的值（字节串）
            serialized_value_bytes = self.file.read(length)
            # 解码为字符串
            serialized_value_str = serialized_value_bytes.decode('utf-8')
            # 使用KeyValue的deserialize方法还原对象
            return KeyValue.deserialize(serialized_value_str)

        self.file.close()  # 关闭文件
        return None

    def read(self, key: str) -> Optional[str]:
        self.file = open(self.path, 'rb')  # 重新以二进制读取模式打开文件
        index_position, index_length = self._read_metadata()  # 读取元数据
        index = self._read_index(index_position, index_length)  # 读取索引
        if key in index:  # 如果键在索引中
            position, length = index[key]
            value = self._read_data(position, length)  # 读取数据
            self.file.close()
            return value
        self.file.close()
        return None

    def _read_metadata(self):
        # 跳转到文件的末尾前256个字节，即元数据区域的起始位置
        self.file.seek(-256, 2)
        # 读取256字节的元数据
        metadata_bytes = self.file.read(256)
        # 将字节解码为UTF-8字符串，并解析为JSON对象
        metadata = json.loads(metadata_bytes.decode('utf-8'))
        # 返回索引位置和索引长度
        return metadata['index_position'], metadata['index_length']

    def _read_index(self, index_position: int, index_length: int):
        # 跳转到索引位置
        self.file.seek(index_position)
        # 读取指定长度的索引字节
        index_bytes = self.file.read(index_length)
        # 将字节解码为UTF-8字符串，并解析为JSON对象
        index = json.loads(index_bytes.decode('utf-8'))
        # 返回索引
        return index

    def _read_data(self, position: int, length: int):
        # 跳转到指定位置
        self.file.seek(position)
        # 读取指定长度的数据字节
        data_bytes = self.file.read(length)
        # 将字节解码为UTF-8字符串，并解析为JSON对象
        # 返回值字段
        return json.loads(data_bytes.decode('utf-8'))['value']

    @staticmethod
    def merge(sstables: List['SSTable'], output_path: str):
        # 创建一个新的SSTable用于输出
        output_sstable = SSTable(output_path)
        # 用于存储所有输入SSTables的数据
        all_data = []
        for sstable in sstables:
            sstable.file = open(sstable.path, 'rb')  # 重新打开文件以读取
            index_position, index_length = sstable._read_metadata()
            index = sstable._read_index(index_position, index_length)
            for key, (position, length) in index.items():
                value = sstable._read_data(position, length)
                all_data.append((key, value))  # 收集所有数据
            sstable.file.close()
        # 按键排序
        all_data.sort(key=lambda x: x[0])
        # 写入输出SSTable，跳过重复的键
        last_key = None
        for key, value in all_data:
            if key != last_key:
                output_sstable.write_from_memtable({key: value})
                last_key = key  # 更新上一个键
        output_sstable.file.close()  # 关闭文件
