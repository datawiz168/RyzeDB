from data_model import KeyValue  # 从data_model模块导入KeyValue类
import json  # 导入JSON库，用于序列化和反序列化字典
import threading  # 导入线程库，用于同步操作
from typing import Optional  # 导入Optional类型，用于类型注解
from typing import List  #List是Python的类型注解之一，用于指定一个列表，列表中的元素类型可以进一步指定。

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
        with self.lock:  # 获取锁，确保线程安全
            with open('memtable.json', 'w') as file:  # 打开文件以写入
                # 使用键值对的serialize方法将字典写入文件
                json.dump({k: v.serialize() for k, v in self.data.items()}, file)
            self.data.clear()  # 清空字典

    def recover(self, log_path: str):
        try:  # 尝试打开文件
            with open(log_path, 'r') as file:  # 打开文件以读取
                data = json.load(file)  # 使用json.load从文件加载字典
                # 使用键值对的deserialize方法从解析的数据中恢复对象
                self.data = {k: KeyValue.deserialize(v) for k, v in data.items()}
        except FileNotFoundError:  # 捕获文件未找到错误
            print(f"Log file {log_path} not found. Starting with an empty MemTable.")  # 打印错误消息

class SSTable:
    def __init__(self, path: str):
        self.path = path  # SSTable文件的路径
        self.file = open(path, 'wb')  # 以二进制写入模式打开文件
        self.index = {}  # 索引字典，用于存储键的位置和长度

    def write_from_memtable(self, memtable):
        # 获取并排序MemTable的数据
        data = sorted(memtable.data.items(), key=lambda x: x[0])
        # 写入数据区域并创建索引
        for key, value in data:
            position = self.file.tell()  # 获取当前文件位置
            serialized_value = json.dumps({'key': key, 'value': value})
            self.file.write(serialized_value.encode('utf-8'))  # 写入序列化的值
            self.index[key] = (position, len(serialized_value))  # 添加索引
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
        self.file.seek(-256, 2)  # 跳转到元数据区域的起始位置，假设元数据区域固定为256字节
        metadata_bytes = self.file.read(256)
        metadata = json.loads(metadata_bytes.decode('utf-8'))
        return metadata['index_position'], metadata['index_length']

    def _read_index(self, index_position: int, index_length: int):
        self.file.seek(index_position)
        index_bytes = self.file.read(index_length)
        index = json.loads(index_bytes.decode('utf-8'))
        return index

    def _read_data(self, position: int, length: int):
        self.file.seek(position)
        data_bytes = self.file.read(length)
        return json.loads(data_bytes.decode('utf-8'))['value']

    @staticmethod
    def merge(sstables: List['SSTable'], output_path: str):
        # 创建一个新的SSTable用于输出
        output_sstable = SSTable(output_path)
        # 读取所有输入SSTables的数据
        all_data = []
        for sstable in sstables:
            sstable.file = open(sstable.path, 'rb')  # 重新打开文件以读取
            index_position, index_length = sstable._read_metadata()
            index = sstable._read_index(index_position, index_length)
            for key, (position, length) in index.items():
                value = sstable._read_data(position, length)
                all_data.append((key, value))
            sstable.file.close()
        # 按键排序
        all_data.sort(key=lambda x: x[0])
        # 写入输出SSTable，跳过重复的键
        last_key = None
        for key, value in all_data:
            if key != last_key:
                output_sstable.write_from_memtable({key: value})
                last_key = key
        output_sstable.file.close()



# 示例代码
memtable = MemTable(threshold=3)  # 创建MemTable对象，并设置阈值为3
memtable.put('name', KeyValue('name', 'Alice'))  # 插入键值对
memtable.put('age', KeyValue('age', '30'))  # 插入键值对
print(memtable.get('name'))  # 获取并打印值
memtable.put('city', KeyValue('city', 'New York'))  # 插入键值对，触发compact
memtable.recover('memtable.json')  # 从文件恢复数据
print(memtable.get('name'))  # 获取并打印恢复后的值
