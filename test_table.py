from table import MemTable, SSTable  # 从table模块导入MemTable和SSTable类
from data_model import KeyValue      # 从data_model模块导入KeyValue类

def test_memtable():                 # 定义用于测试MemTable的函数
    print("Testing MemTable...")
    memtable = MemTable(threshold=3) # 创建MemTable实例，阈值设置为3
    print("Putting 'name'...")        
    memtable.put('name', KeyValue('name', 'Alice')) # 将'name'键值对插入MemTable
    print("Putting 'age'...")         
    memtable.put('age', KeyValue('age', '30'))      # 将'age'键值对插入MemTable
    print("Getting 'name'...")        
    assert memtable.get('name').value == 'Alice'    # 获取'name'键值对并验证其值
    print("Putting 'city'...")        
    memtable.put('city', KeyValue('city', 'New York')) # 将'city'键值对插入MemTable，触发compact
    print("Recovering from file...")  
    memtable.recover('memtable.json') # 从文件恢复MemTable的状态
    print("Getting 'name' again...")  
    assert memtable.get('name').value == 'Alice'    # 再次获取并验证'name'键值对
    print("MemTable tests passed.")  # 打印MemTable测试通过消息

def test_sstable():                  # 定义用于测试SSTable的函数
    print("Testing SSTable...")
    memtable = MemTable()            # 创建MemTable实例
    print("Putting 'name' and 'age'...")            
    memtable.put('name', KeyValue('name', 'Alice')) # 将'name'键值对插入MemTable
    memtable.put('age', KeyValue('age', '30'))      # 将'age'键值对插入MemTable

    print("Creating SSTable...")      
    sstable = SSTable('sstable_test.bin')           # 创建SSTable实例
    print("Writing from MemTable to SSTable...")     
    sstable.write_from_memtable(memtable.data)      # 将MemTable的内容写入SSTable

    print("Reading from SSTable...")
    assert sstable.get('name').value == 'Alice' # 从SSTable读取并断言'name'键值对的值

    # 在此处添加更多的SSTable操作测试
    # ...

    print("SSTable tests passed.")   # 打印SSTable测试通过消息

if __name__ == "__main__":
    print("Starting tests...")       # 打印开始测试消息
    test_memtable()                  # 执行MemTable测试
    test_sstable()                   # 执行SSTable测试
    print("All tests passed.")       # 打印所有测试通过消息
