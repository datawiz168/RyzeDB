'''
作者：datawiz168
版本说明：基础版本
'''
# 导入所需的库
from collections import deque
from sortedcontainers import SortedDict
import os


# SSTable类表示一个SSTable（排序字符串表）文件
class SSTable:
    def __init__(self, filename):
        self.filename = filename  # 文件名

    def write(self, data):
        with open(self.filename, 'w') as f:  # 打开文件进行写入
            for k, v in data.items():  # 遍历数据中的键值对
                f.write(f"{k} {v}\n")  # 将键值对写入文件

    def read(self, key):
        with open(self.filename, 'r') as f:  # 打开文件进行读取
            for line in f:  # 遍历文件中的每一行
                k, v = line.strip().split()  # 分割键值对
                if k == key:  # 如果找到匹配的键，返回值
                    return v
        return None  # 如果未找到匹配的键，返回None


# LSMT类表示一个基本的LSM-Tree存储引擎
class LSMT:
    def __init__(self, memtable_threshold=5, sstable_path="sstable.txt"):
        self.memtable = SortedDict()  # 内存中的表（memtable），用SortedDict表示
        self.sstables = deque()  # SSTable的队列
        self.memtable_threshold = memtable_threshold  # memtable的大小阈值
        self.sstable_path = sstable_path  # SSTable文件的路径

    def _flush(self):  # 将memtable刷新到SSTable的方法
        if len(self.memtable) >= self.memtable_threshold:  # 检查memtable是否达到阈值
            filename = f"{self.sstable_path}_{len(self.sstables)}.txt"  # 创建新SSTable的文件名
            sstable = SSTable(filename)  # 创建SSTable对象
            sstable.write(self.memtable)  # 将memtable的内容写入SSTable
            self.sstables.appendleft(sstable)  # 将新SSTable添加到队列
            self.memtable.clear()  # 清空memtable

    def put(self, key, value):  # 插入键值对的方法
        self.memtable[key] = value  # 将键值对插入memtable
        self._flush()  # 检查并可能刷新memtable

    def get(self, key):  # 获取键对应值的方法
        value = self.memtable.get(key)  # 从memtable中获取值
        if value is not None:
            return value

        for sstable in self.sstables:  # 遍历SSTables
            value = sstable.read(key)  # 从SSTable中获取值
            if value is not None:
                return value

        return None  # 如果未找到键，返回None


# 使用示例
db = LSMT()
db.put("a", "1")
db.put("b", "2")
print(db.get("a"))  # 输出 "1"
print(db.get("b"))  # 输出 "2"

# 创建一个LSM-Tree实例，将memtable的阈值设置为2
db = LSMT(memtable_threshold=2)

# 插入两个键值对，此时memtable的大小还未达到阈值
db.put("a", "1")
db.put("b", "2")

# 插入第三个键值对，此时将触发刷新操作，并将memtable的内容写入新的SSTable
db.put("c", "3")

# 现在，可以从SSTable或memtable中获取值
print(db.get("a")) # 输出 "1"
print(db.get("b")) # 输出 "2"
print(db.get("c")) # 输出 "3"

first_sstable = db.sstables[-1]
key = "a"
value = first_sstable.read(key)
print(value) # 输出 "1"
