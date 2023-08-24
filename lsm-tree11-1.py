'''
增加统计信息。
'''
# 导入deque和OrderedDict用于数据结构
from collections import deque, OrderedDict
# 导入SortedDict用于排序字典
from sortedcontainers import SortedDict
# 导入os用于文件操作
import os
# 导入time用于时间戳
import time
# 导入glob用于文件匹配
import glob

# 定义SSTable类，用于管理SSTable文件
class SSTable:
    def __init__(self, filename):
        self.filename = filename  # 初始化文件名

    def write(self, data):
        with open(self.filename, 'w') as f:  # 打开文件进行写入
            for k, v in data.items():  # 遍历数据项
                f.write(f"{k} {v}\n")  # 写入键值对

    def read(self, key):
        with open(self.filename, 'r') as f:  # 打开文件进行读取
            for line in f:  # 遍历文件的每一行
                k, v = line.strip().split()  # 分割键和值
                if k == key:  # 如果键匹配
                    return v  # 返回值
        return None  # 如果未找到键，返回None

# 定义LSMT（Log-Structured Merge-Tree）类
class LSMT:
    TOMBSTONE = "TOMBSTONE"  # 定义墓碑值，用于标记删除的键

    def __init__(self, memtable_threshold=5, sstable_thresholds=[5, 10], merge_count=2, cache_size=100, sstable_path="sstable.txt"):
        self.memtable = SortedDict()  # 初始化内存表
        self.sstables = [deque() for _ in range(len(sstable_thresholds))]  # 初始化SSTables的层级结构
        self.memtable_threshold = memtable_threshold  # 设置memtable的阈值
        self.sstable_thresholds = sstable_thresholds  # 设置SSTables的阈值
        self.merge_count = merge_count  # 设置合并操作的数量
        self.cache_size = cache_size  # 设置缓存大小
        self.sstable_path = sstable_path  # 设置SSTable文件路径
        self.cache = OrderedDict()  # 初始化缓存

    def _flush(self):
        if len(self.memtable) >= self.memtable_threshold:  # 如果memtable达到阈值
            filename = f"{self.sstable_path}_{len(self.sstables[0])}.txt"  # 创建新的SSTable文件名
            sstable = SSTable(filename)  # 创建SSTable对象
            sstable.write(self.memtable)  # 将memtable写入SSTable
            self.sstables[0].appendleft(sstable)  # 将新的SSTable添加到第一层
            self.memtable.clear()  # 清空memtable
            self._check_compaction(0)  # 检查是否需要压缩

    def _update_cache(self, key, value):
        self.cache[key] = value  # 更新缓存
        if len(self.cache) > self.cache_size:  # 如果缓存超过大小
            self.cache.popitem(last=False)  # 删除最旧的缓存项

    def put(self, key, value):
        self.memtable[key] = value  # 将键值对放入memtable
        self._update_cache(key, value)  # 更新缓存
        self._flush()  # 检查是否需要刷新memtable

    def update(self, key, value):
        self.put(key, value)  # 更新等同于插入

    def get(self, key):
        value = self.cache.get(key)  # 首先在缓存中查找
        if value is not None:  # 如果在缓存中找到
            if value == self.TOMBSTONE:  # 如果值是墓碑值
                return None  # 返回None
            self._update_cache(key, value)  # 更新缓存
            return value  # 返回值

        value = self.memtable.get(key)  # 在memtable中查找
        if value is not None:  # 如果在memtable中找到
            if value == self.TOMBSTONE:  # 如果值是墓碑值
                return None  # 返回None
            self._update_cache(key, value)  # 更新缓存
            return value  # 返回值

        # 在SSTables中查找
        for level_sstables in self.sstables:
            for sstable in level_sstables:
                value = sstable.read(key)  # 在每个SSTable中查找
                if value is not None:  # 如果找到
                    if value == self.TOMBSTONE:  # 如果值是墓碑值
                        return None  # 返回None
                    self._update_cache(key, value)  # 更新缓存
                    return value  # 返回值
        return None  # 如果未找到，返回None

    def delete(self, key):
        self.put(key, self.TOMBSTONE)  # 删除键值对

    def range_query(self, start_key, end_key):
        result = SortedDict()  # 初始化结果

        # 在memtable中检查范围查询
        for key, value in self.memtable.irange(start_key, end_key):
            if value != self.TOMBSTONE:  # 如果值不是墓碑值
                result[key] = value  # 将键值对添加到结果

        # 在SSTables中检查范围查询
        for level_sstables in reversed(self.sstables):
            for sstable in level_sstables:
                with open(sstable.filename, 'r') as f:  # 打开每个SSTable文件
                    for line in f:  # 遍历每一行
                        k, v = line.strip().split()  # 分割键和值
                        if start_key <= k <= end_key and k not in result:  # 如果键在范围内并且不在结果中
                            result[k] = v  # 将键值对添加到结果

        # 在缓存中检查范围查询
        for key in self.cache:
            if start_key <= key <= end_key:  # 如果键在范围内
                value = self.cache[key]  # 获取值
                if value != self.TOMBSTONE:  # 如果值不是墓碑值
                    result[key] = value  # 将键值对添加到结果

        # 删除标记为TOMBSTONE的键
        for key in list(result.keys()):
            if result[key] == self.TOMBSTONE:  # 如果值是墓碑值
                result.pop(key)  # 从结果中删除键

        return list(result.items())  # 返回结果

    def _compact(self, level):
        print(f"Compacting level {level}")  # 打印压缩级别
        merged_data = SortedDict()  # 初始化合并数据
        for _ in range(min(self.merge_count, len(self.sstables[level]))):  # 遍历要合并的SSTables
            sstable = self.sstables[level].popleft()  # 获取SSTable
            with open(sstable.filename, 'r') as f:  # 打开SSTable文件
                for line in f:  # 遍历每一行
                    k, v = line.strip().split()  # 分割键和值
                    if v != self.TOMBSTONE:  # 如果值不是墓碑值
                        merged_data[k] = v  # 将键值对添加到合并数据
            os.remove(sstable.filename)  # 删除旧的SSTable文件

        timestamp = int(time.time_ns())  # 获取时间戳
        new_filename = f"{self.sstable_path}_merged_{level}_{timestamp}.txt"  # 创建新的SSTable文件名
        new_sstable = SSTable(new_filename)  # 创建新的SSTable对象
        new_sstable.write(merged_data)  # 将合并数据写入新的SSTable

        self.sstables[level].appendleft(new_sstable)  # 将新的SSTable添加到级别

    def _check_compaction(self, level):
        if len(self.sstables[level]) >= self.sstable_thresholds[level]:  # 如果SSTables达到阈值
            self._compact(level)  # 进行压缩
            if level + 1 < len(self.sstables):  # 如果还有下一级
                self.sstables[level + 1].append(self.sstables[level].popleft())  # 将SSTable移动到下一级
                self._check_compaction(level + 1)  # 检查下一级是否需要压缩

    def get_stats(self):
        stats = {
            'memtable_size': len(self.memtable),  # 获取memtable大小
            'sstable_count': sum(len(level) for level in self.sstables),  # 获取SSTables数量
            'cache_size': len(self.cache),  # 获取缓存大小
            # 其他统计信息
        }
        return stats  # 返回统计信息

# 测试代码
for filename in glob.glob('sstable.txt*'):
    os.remove(filename)  # 删除旧的SSTable文件

db = LSMT(memtable_threshold=2, sstable_thresholds=[10, 50, 100], merge_count=5)  # 创建LSMT对象

# 插入一些数据
db.put("key1", "value1")
db.put("key2", "value2")
db.put("key3", "value3")

# 插入后检查统计信息
stats = db.get_stats()
print("Stats after insertion:", stats)  # 打印统计信息
assert stats['memtable_size'] == 1
assert stats['sstable_count'] == 1
assert stats['cache_size'] == 3
print("All tests passed!")  # 打印测试通过消息
