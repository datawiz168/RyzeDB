from table import MemTable, SSTable
from persistence import PersistenceManager
from compaction import CompactionStrategy

class LSMT:
    def __init__(self,
                 memtable_threshold: int = 1000,
                 merge_threshold: int = 3,
                 sstable_filename: str = "sstable.data"):
        self.memtable = MemTable(threshold=memtable_threshold)  # 内存表初始化
        self.sstable = SSTable(filename=sstable_filename)  # 排序字符串表
        self.compaction_strategy = CompactionStrategy(merge_threshold=merge_threshold)  # 压缩策略初始化
        self.persistence_manager = PersistenceManager()  # 持久化管理初始化

    def put(self, key: str, value: str):
        self.memtable.put(key, value)
        # 检查是否需要压缩
        if self.compaction_strategy.needs_compaction(self.memtable, self.sstable):
            self.compact()

    def get(self, key: str):
        value = self.memtable.get(key)
        if value is None:
            value = self.sstable.get(key)
        return value

    def delete(self, key: str):
        self.memtable.delete(key)  # 从MemTable中删除键
        # 若需要，可在SSTable中标记键为已删除，这部分可能需要在SSTable中添加逻辑

    def compact(self):
        # 执行压缩策略
        self.compaction_strategy.compact(self.memtable, self.sstable)
        # 持久化MemTable和SSTable
        self.persistence_manager.persist(self.memtable, self.sstable)

    def recover(self):
        # 从持久化存储中恢复MemTable和SSTable
        self.persistence_manager.recover(self.memtable, self.sstable)
