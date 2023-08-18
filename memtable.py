from data_model import KeyValue  # 从data_model模块导入KeyValue类
import json  # 导入JSON库，用于序列化和反序列化字典
import threading  # 导入线程库，用于同步操作
from typing import Optional  # 导入Optional类型，用于类型注解

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

# 示例代码
memtable = MemTable(threshold=3)  # 创建MemTable对象，并设置阈值为3
memtable.put('name', KeyValue('name', 'Alice'))  # 插入键值对
memtable.put('age', KeyValue('age', '30'))  # 插入键值对
print(memtable.get('name'))  # 获取并打印值
memtable.put('city', KeyValue('city', 'New York'))  # 插入键值对，触发compact
memtable.recover('memtable.json')  # 从文件恢复数据
print(memtable.get('name'))  # 获取并打印恢复后的值
