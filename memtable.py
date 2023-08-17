from typing import Optional

# MemTable是LSM-tree结构的一部分，用于存储最近写入的数据。
# 当数据达到一定阈值时，MemTable的内容将持久化到SSTable。
# 这种设计是为了优化写入性能，使其能够快速响应写入请求。
class MemTable:
    # 构造函数初始化MemTable，并设置持久化阈值。
    # threshold参数定义了触发持久化操作的键值对数量。
    def __init__(self, threshold: int):
        self.data = {}  # 使用字典存储键值对，提供O(1)的读写性能。
        self.threshold = threshold  # 设置持久化阈值。

    # put方法用于插入或更新键值对。
    # 如果键不存在，将创建新的键值对；如果键已存在，将更新对应的值。
    def put(self, key: str, value: str):
        self.data[key] = value  # 插入或更新键值对。
        # 如果达到持久化阈值，触发compact方法进行持久化操作。
        if len(self.data) >= self.threshold:
            self.compact()

    # get方法用于检索给定键的值。
    # 如果键不存在，将返回None。
    def get(self, key: str) -> Optional[str]:
        return self.data.get(key)  # 使用字典的get方法获取值。

    # delete方法用于删除给定键的键值对。
    # 如果键不存在，操作无效。
    def delete(self, key: str):
        if key in self.data:  # 检查键是否存在。
            del self.data[key]  # 删除键值对。

    # compact方法负责将MemTable的内容持久化到SSTable。
    # 在LSM-tree中，这是将数据从内存转移到磁盘的关键步骤。
    # 这个方法在实际实现中需要根据SSTable的设计来完成。
    def compact(self):
        # TODO: 实现持久化逻辑。
        pass

# 示例代码演示了如何使用MemTable进行基本操作。
memtable = MemTable(threshold=3)
memtable.put('name', 'Alice')
memtable.put('age', '30')
print(memtable.get('name'))  # 输出 'Alice'
