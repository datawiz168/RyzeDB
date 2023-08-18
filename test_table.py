from table import MemTable, SSTable  # 导入MemTable和SSTable类
from data_model import KeyValue      # 导入KeyValue类

def test_memtable():                 # 定义MemTable测试函数
    memtable = MemTable(threshold=3) # 创建MemTable实例，设置阈值为3
    memtable.put('name', KeyValue('name', 'Alice')) # 插入'name'键值对
    memtable.put('age', KeyValue('age', '30'))      # 插入'age'键值对
    assert memtable.get('name').value == 'Alice'    # 断言检查'name'的值
    memtable.put('city', KeyValue('city', 'New York')) # 插入'city'键值对
    memtable.recover('memtable.json')              # 从文件恢复数据
    assert memtable.get('name').value == 'Alice'   # 再次检查'name'的值
    print("MemTable tests passed.")                # 打印测试通过消息

def test_sstable():                  # 定义SSTable测试函数
    memtable = MemTable()            # 创建MemTable实例
    memtable.put('name', KeyValue('name', 'Alice')) # 插入'name'键值对
    memtable.put('age', KeyValue('age', '30'))      # 插入'age'键值对
    
    sstable = SSTable('sstable_test.bin')           # 创建SSTable实例
    sstable.write_from_memtable(memtable.data)      # 从MemTable写入SSTable

    # 在此处添加更多的SSTable操作测试
    # ...

    print("SSTable tests passed.")                 # 打印测试通过消息

if __name__ == "__main__":           # 如果直接运行此文件
    test_memtable()                  # 执行MemTable测试
    test_sstable()                   # 执行SSTable测试
    print("All tests passed.")       # 打印所有测试通过消息
