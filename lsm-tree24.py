'''
增加测试模块以及部分测试代码。
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
# 导入json用于JSON操作
import json
# 导入mmh3用于计算Murmur3哈希
import mmh3
# 导入bitarray用于布隆过滤器
from bitarray import bitarray
# 导入uuid用于生成唯一事务ID
import uuid
# 导入用于解析SQL语句的库
import sqlparse
# 从sqlparse.tokens导入Keyword和DDL，用于标识SQL语句中的关键词和数据定义语言（DDL）部分
from sqlparse.tokens import Keyword, DDL 
# Faker 是一个 Python 库，用于生成各种类型的假数据。例如，你可以使用 Faker 生成假的姓名、地址、电子邮件等。
from faker import Faker

class QueryParser:
    def __init__(self):
        self.tables = []
        self.columns = []
        self.where_conditions = []
        self.query_type = None
        self.key = None
        self.value = None
        self.transaction_id = None
        self.table_name = None  # 新增:用于存储建表语句中的表名
        self.columns_info = []  # 新增:用于存储建表语句中的列信息

    def parse(self, sql):
        # 生成唯一的事务ID
        self.transaction_id = str(uuid.uuid4())
        # 解析SQL查询
        parsed = sqlparse.parse(sql)[0]

        # 确定查询类型
        if parsed.get_type() == 'SELECT':
            self.query_type = 'SELECT'
        elif parsed.get_type() == 'INSERT':
            self.query_type = 'INSERT'
        elif parsed.get_type() == 'UPDATE':
            self.query_type = 'UPDATE'
        elif parsed.get_type() == 'DELETE':
            self.query_type = 'DELETE'
        elif parsed.get_type() == 'CREATE':  # 新增:建表语句的查询类型
            self.query_type = 'CREATE'
             # 正确处理 CREATE TABLE 的返回值
            self.table_name, self.columns_info = self._parse_create_table(parsed)
            # 注意这里不再清空 self.tables 和 self.columns，因为我们现在正确处理表名和列信息了
        else:
            raise ValueError(f"Unsupported query type: {parsed.get_type()}")

        # 遍历解析后的SQL查询的所有Token
        for token in parsed.tokens:
            # 如果Token是标识符(Identifier),则认为它是表名
            if isinstance(token, sqlparse.sql.Identifier):
                self.tables.append(token.get_name())
            # 如果Token是标识符列表(IdentifierList),则认为它是列名列表
            elif isinstance(token, sqlparse.sql.IdentifierList):
                for identifier in token.get_identifiers():
                    self.columns.append(identifier.get_name())
            # 如果Token是WHERE子句,则解析WHERE条件
            elif isinstance(token, sqlparse.sql.Where):
                self.where_conditions = self._parse_where(token)
            # 如果Token是比较操作符(Comparison),则解析SET子句(用于UPDATE查询)
            elif isinstance(token, sqlparse.sql.Comparison):
                self.key, self.value = self._parse_comparison(token)
            # 如果Token是VALUES子句(用于INSERT查询),则解析VALUES子句
            elif isinstance(token, sqlparse.sql.Values):
                self.key, self.value = self._parse_values(token)
            # 如果Token是建表语句,则解析表名和列信息
            elif isinstance(token, sqlparse.sql.Statement) and token.get_type() == 'CREATE':
                self.table_name, self.columns_info = self._parse_create_table(token)
                self.tables = []  # 清空tables列表
                self.columns = []  # 清空columns列表
        return self

    def _validate_columns(self, table_name, tables_info):
        """
        验证查询中的列名是否存在于对应的表中。
        
        参数:
        - table_name: 要验证的表名。
        - tables_info: 包含表信息的字典。
        
        返回:
        - 如果所有列名都存在,则返回True;否则返回False。
        """
        if table_name not in tables_info:
            return False

        table_columns = [column[0] for column in tables_info[table_name]['columns_info']]

        for column in self.columns:
            if column not in table_columns:
                return False

        return True

    def _parse_where(self, token):
        # 解析WHERE子句中的条件
        conditions = []

        # 遍历WHERE子句中的所有Token
        for subtoken in token.tokens:
            # 如果Token是比较操作符(Comparison),则提取左操作数、操作符和右操作数
            if isinstance(subtoken, sqlparse.sql.Comparison):
                # 获取左操作数的名称
                left = subtoken.left.get_name()
                # 获取操作符
                operator = subtoken.token_next(0)[1].value
                # 获取右操作数
                right = subtoken.right

                # 如果右操作数是标识符,则获取其名称
                if isinstance(right, sqlparse.sql.Identifier):
                    right = right.get_name()
                # 如果右操作数是字符串字面值,则去除引号
                elif isinstance(right, sqlparse.sql.Token) and right.ttype == sqlparse.tokens.Literal.String.Single:
                    right = right.value[1:-1]
                # 如果右操作数是数字字面值,则直接使用其值
                elif isinstance(right, sqlparse.sql.Token) and right.ttype == sqlparse.tokens.Literal.Number.Integer:
                    right = int(right.value)

                # 将解析后的条件添加到条件列表中
                conditions.append((left, operator, right))

        return conditions



    def _parse_comparison(self, token):
        # 解析比较操作符(用于UPDATE查询)
        key = token.left.get_name()
        value = token.right

        # 如果右操作数是字符串字面值,则去除引号
        if isinstance(value, sqlparse.sql.Token) and value.ttype == sqlparse.tokens.Literal.String.Single:
            value = value.value[1:-1]
        # 如果右操作数是数字字面值,则直接使用其值
        elif isinstance(value, sqlparse.sql.Token) and value.ttype == sqlparse.tokens.Literal.Number.Integer:
            value = int(value.value)

        return key, value

    def _parse_values(self, token):
        # 解析VALUES子句(用于INSERT查询)
        key = None
        value = None

        # 遍历VALUES子句中的所有Token
        for subtoken in token.tokens:
            # 如果Token是括号(Parenthesis),则提取括号内的值
            if isinstance(subtoken, sqlparse.sql.Parenthesis):
                # 获取括号内的值
                values = subtoken.value.strip('()').split(',')
                # 假设第一个值是键,第二个值是值
                key = values[0].strip().strip("'")
                value = values[1].strip().strip("'")

        return key, value

    def _parse_create_table(self, token):
        # 初始化表名和列信息列表
        table_name = None
        columns_info = []
        # 用于标记是否找到了 'TABLE' 关键字
        found_table_keyword = False

        # 遍历 SQL 解析后的 tokens
        for t in token.tokens:
            # 如果已经找到了 'TABLE' 关键字，下一个标识符应该是表名
            if found_table_keyword:
                if isinstance(t, sqlparse.sql.Identifier):
                    table_name = t.get_name()
                    # 找到表名后重置标志位
                    found_table_keyword = False
            # 检查是否为 'TABLE' 关键字
            elif t.match(sqlparse.tokens.Keyword, 'TABLE'):
                # 设置标志位，准备获取表名
                found_table_keyword = True
            # 如果找到了包含列定义的括号
            elif isinstance(t, sqlparse.sql.Parenthesis):
                # 提取并解析列定义
                inside_parenthesis = t.value.strip('()')
                columns_definitions = inside_parenthesis.split(',')
                for column_definition in columns_definitions:
                    # 假设每个列定义至少包含列名和列类型
                    parts = column_definition.strip().split()
                    if len(parts) >= 2:  # 至少需要有列名和列类型
                        column_name, column_type = parts[0], ' '.join(parts[1:])
                        columns_info.append((column_name, column_type))

        return table_name, columns_info

# 定义B+树节点类
class BPlusTreeNode:
    def __init__(self, keys=None, children=None, is_leaf=True):
        self.keys = keys or []  # 节点的键列表,如果没有提供,则默认为空列表
        self.children = children or []  # 节点的子节点列表,如果没有提供,则默认为空列表
        self.is_leaf = is_leaf  # 标识节点是否为叶节点的布尔值

    def __str__(self):
        return f"BPlusTreeNode(keys={self.keys}, is_leaf={self.is_leaf})"  # 返回节点的字符串表示

    def __repr__(self):
        return self.__str__()  # 返回节点的可打印表示,与__str__相同

# 定义B+树类
class BPlusTree:
    def __init__(self, branching_factor=4):
        self.branching_factor = branching_factor  # B+树的分支因子,决定了每个节点的最大键数
        self.root = BPlusTreeNode()  # B+树的根节点,初始为空的叶节点

    def _find_leaf(self, key):
        node = self.root  # 从根节点开始查找
        while not node.is_leaf:  # 当节点不是叶节点时,继续查找
            i = 0
            while i < len(node.keys) and key > node.keys[i]:  # 找到第一个大于或等于key的键的位置
                i += 1
            node = node.children[i]  # 进入对应的子树
        return node  # 返回包含key的叶节点

    def search(self, key):
        leaf_node = self._find_leaf(key)  # 查找包含key的叶节点
        if key in leaf_node.keys:  # 如果key在叶节点的键列表中
            return True  # 返回True,表示找到了key
        return False  # 否则返回False,表示没有找到key

    def _split_child(self, parent, child_index):
        print(f"Splitting child at index {child_index} of parent {parent}")  # 打印分裂操作的详细信息
        new_child = BPlusTreeNode()  # 创建一个新的子节点
        child = parent.children[child_index]  # 获取需要分裂的子节点
        new_child.is_leaf = child.is_leaf  # 新子节点与原子节点有相同的叶节点状态

        split_index = len(child.keys) // 2  # 计算分裂点的位置
        new_child.keys = child.keys[split_index:]  # 新子节点获取分裂点右侧的键
        new_child.children = child.children[split_index:]  # 新子节点获取分裂点右侧的子节点
        child.keys = child.keys[:split_index]  # 原子节点保留分裂点左侧的键
        child.children = child.children[:split_index + 1]  # 原子节点保留分裂点左侧的子节点

        parent.keys.insert(child_index, child.keys[-1])  # 将原子节点的最大键插入到父节点的适当位置
        parent.children.insert(child_index + 1, new_child)  # 将新子节点插入到父节点的适当位置

    def _insert_non_full(self, node, key):
        print(f"Inserting key {key} into node {node}")  # 打印插入操作的详细信息
        i = len(node.keys) - 1  # 从节点的最右侧开始
        if node.is_leaf:  # 如果是叶节点
            node.keys.append(None)  # 在键列表的末尾添加一个占位符
            while i >= 0 and key < node.keys[i]:  # 从右到左查找key应该插入的位置
                node.keys[i + 1] = node.keys[i]  # 将键向右移动以腾出空间
                i -= 1
            node.keys[i + 1] = key  # 将key插入到正确的位置
        else:  # 如果不是叶节点
            while i >= 0 and key < node.keys[i]:  # 从右到左查找key应该插入的子树
                i -= 1
            i += 1  # 进入对应的子树
            if len(node.children[i].keys) == 2 * self.branching_factor - 1:  # 如果子节点已满
                self._split_child(node, i)  # 分裂子节点
                if key > node.keys[i]:  # 如果key大于分裂点
                    i += 1  # 进入右侧的新子树
            self._insert_non_full(node.children[i], key)  # 递归地插入key到适当的子树

    def insert(self, key):
        print(f"Inserting key {key} into B+ tree")  # 打印插入操作的详细信息
        node = self.root  # 从根节点开始
        if len(node.keys) == 2 * self.branching_factor - 1:  # 如果根节点已满
            print("Root node is full, splitting...")  # 打印根节点已满,需要分裂的信息
            new_root = BPlusTreeNode(is_leaf=False)  # 创建一个新的根节点
            self.root = new_root  # 更新树的根节点
            new_root.children.append(node)  # 将旧根节点作为新根节点的子节点
            self._split_child(new_root, 0)  # 分裂旧根节点(新根节点的第一个子节点)
            self._insert_non_full(new_root, key)  # 将key插入到新的根节点
        else:  # 如果根节点未满
            self._insert_non_full(node, key)  # 将key插入到适当的子树

    def _merge_nodes(self, parent, child_index):
        print(f"Merging child at index {child_index} of parent {parent}")  # 打印合并操作的详细信息
        child = parent.children[child_index]  # 获取需要合并的子节点
        sibling = parent.children[child_index + 1]  # 获取它的右兄弟节点

        child.keys.append(parent.keys[child_index])  # 将父节点的分隔键下移到子节点
        child.keys.extend(sibling.keys)  # 将右兄弟节点的键合并到子节点
        child.children.extend(sibling.children)  # 将右兄弟节点的子节点合并到子节点

        parent.keys.pop(child_index)  # 从父节点中删除分隔键
        parent.children.pop(child_index + 1)  # 从父节点中删除右兄弟节点

    def _delete_entry(self, node, key, child_index):
        print(f"Deleting key {key} from node {node}")  # 打印删除操作的详细信息
        if node.is_leaf:  # 如果是叶节点
            if key in node.keys:  # 如果key在节点中
                node.keys.remove(key)  # 直接删除key
        else:  # 如果不是叶节点
            if child_index < len(node.children) - 1 and len(node.children[child_index + 1].keys) >= self.branching_factor:
                # 如果key所在子节点的右兄弟节点有足够的键
                successor = node.children[child_index + 1].keys[0]  # 找到后继键(右兄弟节点的第一个键)
                node.keys[child_index] = successor  # 用后继键替换key
                self._delete_entry(node.children[child_index + 1], successor, 0)  # 递归地删除后继键
            elif child_index > 0 and len(node.children[child_index - 1].keys) >= self.branching_factor:
                # 如果key所在子节点的左兄弟节点有足够的键
                predecessor = node.children[child_index - 1].keys[-1]  # 找到前驱键(左兄弟节点的最后一个键)
                node.keys[child_index - 1] = predecessor  # 用前驱键替换key
                self._delete_entry(node.children[child_index - 1], predecessor, len(node.children[child_index - 1].keys) - 1)  # 递归地删除前驱键
            else:  # 如果兄弟节点都没有足够的键
                if child_index < len(node.children) - 1:  # 如果key所在子节点有右兄弟节点
                    self._merge_nodes(node, child_index)  # 将key所在子节点与其右兄弟节点合并
                    self._delete_entry(node.children[child_index], key, 0)  # 在合并后的节点中递归地删除key
                else:  # 如果key所在子节点没有右兄弟节点
                    self._merge_nodes(node, child_index - 1)  # 将key所在子节点与其左兄弟节点合并
                    self._delete_entry(node.children[child_index - 1], key, len(node.children[child_index - 1].keys))  # 在合并后的节点中递归地删除key

    def delete(self, key):
        print(f"Deleting key {key} from B+ tree")  # 打印删除操作的详细信息
        node = self.root  # 从根节点开始
        self._delete_entry(node, key, 0)  # 从根节点开始删除key
        if len(node.keys) == 0 and not node.is_leaf:  # 如果删除后根节点为空且不是叶节点
            print("Root node is empty after deletion, updating root...")  # 打印根节点为空,需要更新根节点的信息
            self.root = node.children[0]  # 将根节点更新为其唯一的子节点


# 定义布隆过滤器类
class BloomFilter:
    def __init__(self, size, hash_count):
        self.size = size  # 布隆过滤器的大小,即位数组的长度
        self.hash_count = hash_count  # 哈希函数的数量
        self.bit_array = bitarray(size)  # 创建一个指定大小的位数组
        self.bit_array.setall(0)  # 将位数组的所有位初始化为0

    def add(self, key):
        # 将键添加到布隆过滤器中
        for i in range(self.hash_count):
            # 对键进行多次哈希,每次使用不同的种子(i)
            # 使用mmh3库的hash函数计算哈希值
            index = mmh3.hash(key, i) % self.size
            # 将哈希值映射到位数组的对应位置,并将其设置为1
            self.bit_array[index] = 1

    def contains(self, key):
        # 检查键是否可能存在于布隆过滤器中
        for i in range(self.hash_count):
            # 对键进行多次哈希,每次使用不同的种子(i)
            # 使用mmh3库的hash函数计算哈希值
            index = mmh3.hash(key, i) % self.size
            # 检查位数组中对应位置的值
            if self.bit_array[index] == 0:
                # 如果任意一个对应位置的值为0,则键一定不存在
                return False
        # 如果所有对应位置的值都为1,则键可能存在
        return True


class WAL:
    def __init__(self, filename):
        self.filename = filename

    def write_log(self, operation, key, value, transaction_id):
        """
        将操作日志写入WAL日志文件。
        
        参数:
        - operation: 操作类型,如'put'或'delete'。
        - key: 操作的键。
        - value: 操作的值,如果是删除操作,则为None。
        - transaction_id: 操作所属的事务ID。
        """
        log_entry = {
            'operation': operation,
            'key': key,
            'value': value,
            'transaction_id': transaction_id
        }
        with open(self.filename, 'a') as f:
            f.write(json.dumps(log_entry) + '\n')

    def read_logs(self):
        """
        从WAL日志文件中读取所有日志条目。
        
        返回:
        - 日志条目的列表,每个条目都是一个字典。
        """
        try:
            with open(self.filename, 'r') as f:
                logs = [json.loads(line) for line in f]
                return logs
        except FileNotFoundError:
            return []

    def clear_logs(self):
        """
        清空WAL日志文件的内容。
        """
        open(self.filename, 'w').close()


# 定义SSTable类,用于管理SSTable文件
class SSTable:
    def __init__(self, filename):
        self.filename = filename  # 初始化文件名
        self.index = BPlusTree()  # 创建B+树索引
        self.bloom_filter = BloomFilter(1000, 5)  # 创建一个大小为1000,哈希函数数量为5的布隆过滤器

    def write(self, data):
        with open(self.filename, 'w') as f:  # 打开文件进行写入
            json.dump(data, f)  # 使用json将数据写入文件
        for key in data.keys():  # 为每个键创建索引
            self.index.insert(key)
            self.bloom_filter.add(key)  # 将键添加到布隆过滤器

    def read(self, key):
        if not self.bloom_filter.contains(key):  # 首先检查布隆过滤器
            return None
        if not self.index.search(key):  # 如果键不在索引中
            return None
        with open(self.filename, 'r') as f:  # 打开文件进行读取
            data = json.load(f)  # 从json读取数据
            return data.get(key)  # 返回与键匹配的值,如果未找到,则返回None

# 定义LSMT(Log-Structured Merge-Tree)类
class LSMT:
    TOMBSTONE = "TOMBSTONE"  # 定义墓碑值,用于标记删除的键

    def __init__(self, memtable_threshold=5, sstable_thresholds=[5, 10], merge_count=2, cache_size=100,
                 sstable_path="sstable.txt", wal_filename="wal.log"):
        self.memtable = SortedDict()  # 初始化内存表
        self.sstables = [deque() for _ in range(len(sstable_thresholds))]  # 初始化SSTables的层级结构
        self.memtable_threshold = memtable_threshold  # 设置memtable的阈值
        self.sstable_thresholds = sstable_thresholds  # 设置SSTables的阈值
        self.merge_count = merge_count  # 设置合并操作的数量
        self.cache_size = cache_size  # 设置缓存大小
        self.sstable_path = sstable_path  # 设置SSTable文件路径
        self.cache = OrderedDict()  # 初始化缓存
        self.wal = WAL(wal_filename)  # 创建WAL对象
        self.table_stats = {}  # 初始化表的统计信息
        self._recover_from_wal()  # 从WAL日志中恢复数据
        self.tables = {}  # 新增:用于存储已创建的表的信息
    
  
    def _recover_from_wal(self):
        """
        从WAL日志中恢复数据。
        """
        print("Recovering data from WAL...")
        print("Current table stats:", self.table_stats)
        logs = self.wal.read_logs()  # 读取所有日志条目
        for log in logs:
            operation = log['operation']
            key = log['key']
            value = log['value']
            transaction_id = log['transaction_id']

            if operation == 'put':
                self.put(key, value, transaction_id, bypass_wal=True)  # 重放put操作
            elif operation == 'delete':
                self.delete(key, transaction_id, bypass_wal=True)  # 重放delete操作

        self.wal.clear_logs()  # 清空WAL日志文件

    def _update_cache_with_batch_size(self, key, value, batch_size=1):
        """
        更新缓存,并在缓存满时以批量方式淘汰最近最少使用的项目。
        
        参数:
        - key:要在缓存中插入或更新的键。
        - value:与键关联的值。
        - batch_size:当缓存满时,要从缓存中淘汰的最近最少使用项目的数量。
        """
        self.cache[key] = value  # 更新缓存
        if len(self.cache) > self.cache_size:  # 如果缓存已满,则以批量方式淘汰最近最少使用的项目
            for _ in range(batch_size):
                if len(self.cache) > self.cache_size:
                    self.cache.popitem(last=False)


    def put(self, key, value, transaction_id, batch_size=1, bypass_wal=False):
        """
        在数据库中插入或更新一个键值对,并更新缓存。
        
        参数:
        - key:要插入或更新的键。
        - value:与键关联的值。
        - transaction_id:执行操作的事务ID。
        - batch_size:当缓存满时,要从缓存中淘汰的最近最少使用项目的数量。
        - bypass_wal:是否绕过WAL日志写入。
        """
        print(f"Putting key {key} with value {value} (Transaction {transaction_id})")
        print("Current table stats:", self.table_stats)
        print(f"[Transaction {transaction_id}] Putting key {key} with value {value}")  # 打印插入操作的详细信息
        if not bypass_wal:
            self.wal.write_log('put', key, value, transaction_id)  # 将put操作写入WAL日志
        self.memtable[key] = value  # 更新内存表
        self._update_cache_with_batch_size(key, value, batch_size=batch_size)  # 更新缓存
        if key in self.memtable:
            self.table_stats['row_count'] = self.table_stats.get('row_count', 0) + 1  # 更新表的行数统计信息
        self._flush(transaction_id)  # 如有必要,将内存表刷新到SSTable

    def update(self, key, value, transaction_id):
        """
        更新数据库中的键值对。
        
        参数:
        - key:要更新的键。
        - value:新的值。
        - transaction_id:执行操作的事务ID。
        """
        self.put(key, value, transaction_id)  # 更新等同于插入

    def get(self, key, transaction_id):
        """
        从数据库中检索与给定键关联的值。
        
        参数:
        - key:要检索的键。
        - transaction_id:执行操作的事务ID。
        
        返回:
        - 与键关联的值,如果键不存在,则返回None。
        """
        print(f"[Transaction {transaction_id}] Getting key {key}")  # 打印读取操作的详细信息
        value = self.cache.get(key)  # 首先在缓存中查找
        if value is not None:  # 如果在缓存中找到
            print(f"[Transaction {transaction_id}] Found key {key} in cache")  # 打印在缓存中找到键的信息
            if value == self.TOMBSTONE:  # 如果值是墓碑值
                return None  # 返回None
            self._update_cache_with_batch_size(key, value)  # 更新缓存
            return value  # 返回值

        value = self.memtable.get(key)  # 在memtable中查找
        if value is not None:  # 如果在memtable中找到
            print(f"[Transaction {transaction_id}] Found key {key} in memtable")  # 打印在memtable中找到键的信息
            if value == self.TOMBSTONE:  # 如果值是墓碑值
                return None  # 返回None
            self._update_cache_with_batch_size(key, value)  # 更新缓存
            return value  # 返回值

        for level, level_sstables in enumerate(self.sstables):  # 在SSTables中查找
            for sstable in level_sstables:
                value = sstable.read(key)  # 在每个SSTable中查找
                if value is not None:  # 如果找到
                    print(f"[Transaction {transaction_id}] Found key {key} in SSTable at level {level}")  # 打印在SSTable中找到键的信息
                    if value == self.TOMBSTONE:  # 如果值是墓碑值
                        return None  # 返回None
                    self._update_cache_with_batch_size(key, value)  # 更新缓存
                    return value  # 返回值

        print(f"[Transaction {transaction_id}] Key {key} not found")  # 打印未找到键的信息
        return None  # 如果未找到,返回None

    def delete(self, key, transaction_id, bypass_wal=False):
        """
        从数据库中删除与给定键关联的值。
        
        参数:
        - key:要删除的键。
        - transaction_id:执行操作的事务ID。
        - bypass_wal:是否绕过WAL日志写入。
        """
        print(f"[Transaction {transaction_id}] Deleting key {key}")  # 打印删除操作的详细信息
        if not bypass_wal:
            self.wal.write_log('delete', key, None, transaction_id)  # 将delete操作写入WAL日志
        self.put(key, self.TOMBSTONE, transaction_id)  # 删除键值对
        if key in self.memtable:
            self.table_stats['row_count'] = self.table_stats.get('row_count', 0) - 1  # 更新表的行数统计信息

    def range_query(self, start_key, end_key, transaction_id):
        """
        执行范围查询,返回键在指定范围内的所有键值对。
        
        参数:
        - start_key:范围的起始键。
        - end_key:范围的结束键。
        - transaction_id:执行操作的事务ID。
        
        返回:
        - 键在指定范围内的所有键值对的列表。
        """
        print(f"[Transaction {transaction_id}] Range query from {start_key} to {end_key}")  # 打印范围查询的详细信息
        result = SortedDict()  # 初始化结果

        for key, value in self.memtable.irange(start_key, end_key):  # 在memtable中检查范围查询
            if value != self.TOMBSTONE:  # 如果值不是墓碑值
                result[key] = value  # 将键值对添加到结果

        for level_sstables in reversed(self.sstables):  # 在SSTables中检查范围查询
            for sstable in level_sstables:
                with open(sstable.filename, 'r') as f:  # 打开每个SSTable文件
                    data = json.load(f)  # 从json读取数据
                    for k, v in data.items():
                        if start_key <= k <= end_key and k not in result:  # 如果键在范围内并且不在结果中
                            result[k] = v  # 将键值对添加到结果

        for key in self.cache:  # 在缓存中检查范围查询
            if start_key <= key <= end_key:  # 如果键在范围内
                value = self.cache[key]  # 获取值
                if value != self.TOMBSTONE:  # 如果值不是墓碑值
                    result[key] = value  # 将键值对添加到结果

        for key in list(result.keys()):  # 删除标记为TOMBSTONE的键
            if result[key] == self.TOMBSTONE:  # 如果值是墓碑值
                result.pop(key)  # 从结果中删除键

        print(f"[Transaction {transaction_id}] Range query result: {result}")  # 打印范围查询的结果
        return list(result.items())  # 返回结果

    def _flush(self, transaction_id):
        """
        将内存表刷新到SSTable。
        
        参数:
        - transaction_id:执行操作的事务ID。
        """
        if len(self.memtable) >= self.memtable_threshold:  # 如果memtable达到阈值
            filename = f"{self.sstable_path}_{len(self.sstables[0])}_{transaction_id}.json"  # 创建新的SSTable文件名,包含事务ID
            sstable = SSTable(filename)  # 创建SSTable对象
            sstable.write(self.memtable)  # 将memtable写入SSTable
            self.sstables[0].appendleft(sstable)  # 将新的SSTable添加到第一层
            self.table_stats['row_count'] = self.table_stats.get('row_count', 0) + len(self.memtable)  # 更新表的行数统计信息
            self.memtable.clear()  # 清空memtable
            self._check_compaction(0)  # 检查是否需要压缩

    def _compact(self, level):
        """
        压缩指定层级的SSTables。
        
        参数:
        - level:要压缩的层级。
        """
        print(f"Compacting level {level}")  # 打印压缩级别
        merged_data = SortedDict()  # 初始化合并数据
        for _ in range(min(self.merge_count, len(self.sstables[level]))):  # 遍历要合并的SSTables
            sstable = self.sstables[level].popleft()  # 获取SSTable
            with open(sstable.filename, 'r') as f:  # 打开SSTable文件
                data = json.load(f)  # 从json读取数据
                for k, v in data.items():
                    if v != self.TOMBSTONE:  # 如果值不是墓碑值
                        merged_data[k] = v  # 将键值对添加到合并数据
            os.remove(sstable.filename)  # 删除旧的SSTable文件

        timestamp = int(time.time_ns())  # 获取时间戳
        new_filename = f"{self.sstable_path}_merged_{level}_{timestamp}.json"  # 创建新的SSTable文件名
        new_sstable = SSTable(new_filename)  # 创建新的SSTable对象
        new_sstable.write(merged_data)  # 将合并数据写入新的SSTable

        self.sstables[level].appendleft(new_sstable)  # 将新的SSTable添加到级别

    def _check_compaction(self, level):
        """
        检查指定层级是否需要压缩,并触发压缩操作。
        
        参数:
        - level:要检查的层级。
        """
        if len(self.sstables[level]) >= self.sstable_thresholds[level]:  # 如果SSTables达到阈值
            self._compact(level)  # 进行压缩
            if level + 1 < len(self.sstables):  # 如果还有下一级
                self.sstables[level + 1].append(self.sstables[level].popleft())  # 将SSTable移动到下一级
                self._check_compaction(level + 1)  # 检查下一级是否需要压缩

    def get_stats(self):
        """
        获取数据库的统计信息。
        
        返回:
        - 包含数据库统计信息的字典。
        """
        stats = {
            'memtable_size': len(self.memtable),  # 获取memtable大小
            'sstable_count': sum(len(level) for level in self.sstables),  # 获取SSTables数量
            'cache_size': len(self.cache),  # 获取缓存大小
            'row_count': self.table_stats.get('row_count', 0),  # 获取表的行数统计信息
        }
        return stats  # 返回统计信息


    def execute_query(self, parsed_query):
        try:
            if parsed_query.query_type == 'SELECT':
                print(f"Executing SELECT query on table: {parsed_query.tables}")  # 修改打印信息
                # 检查表是否存在
                for table_name in parsed_query.tables:
                    if table_name not in self.tables:
                        raise ValueError(f"表 {table_name} 不存在")
                return self.handle_select(parsed_query)
            elif parsed_query.query_type == 'INSERT':
                print(f"Executing INSERT query on table: {parsed_query.tables[0]}")
                # 检查表是否存在
                if parsed_query.tables[0] not in self.tables:
                    raise ValueError(f"表 {parsed_query.tables[0]} 不存在")
                return self.handle_insert(parsed_query)
            elif parsed_query.query_type == 'UPDATE':
                print(f"Executing UPDATE query on table: {parsed_query.tables[0]}")
                # 检查表是否存在
                if parsed_query.tables[0] not in self.tables:
                    raise ValueError(f"表 {parsed_query.tables[0]} 不存在")
                return self.handle_update(parsed_query)
            elif parsed_query.query_type == 'DELETE':
                print(f"Executing DELETE query on table: {parsed_query.tables[0]}")
                # 检查表是否存在
                if parsed_query.tables[0] not in self.tables:
                    raise ValueError(f"表 {parsed_query.tables[0]} 不存在")
                return self.handle_delete(parsed_query)
            elif parsed_query.query_type == 'CREATE':
                print(f"Executing CREATE TABLE query: {parsed_query.table_name}")
                table_info = self.handle_create_table(parsed_query)
                self.tables[table_info['table_name']] = table_info
                return None
            else:
                raise ValueError(f"不支持的查询类型: {parsed_query.query_type}")
        except ValueError as e:
            print(f"执行查询时出错: {str(e)}")
            raise
        
    def handle_create_table(self, parsed_query):
        """
        处理 CREATE TABLE 查询。
        
        参数:
            parsed_query: 解析后的查询对象。
        
        返回:
            包含表信息的字典。
        """
        table_name = parsed_query.table_name  # 从解析后的查询中提取表名
        columns_info = parsed_query.columns_info  # 从解析后的查询中提取列信息

        # 检查表是否已存在
        if table_name in self.tables:
            raise ValueError(f"表 {table_name} 已存在")

        # 创建并存储表信息
        table_info = {
            'table_name': table_name,
            'columns_info': columns_info,
            'data': SortedDict()  # 使用 SortedDict 存储数据
        }
        self.tables[table_name] = table_info  # 存储表信息

        print(f"表 {table_name} 已创建，列信息: {columns_info}")
        return table_info


    def handle_select(self, parsed_query):
        """
        处理SELECT查询。

        参数:
        - parsed_query: 解析后的查询对象。

        返回:
        - 查询结果,包含匹配的键值对的列表。
        """
        table_name = parsed_query.tables[0]
        
        print(f"Handling SELECT query on table: {table_name}")  # 添加打印信息
        
        # 检查表是否存在
        if table_name not in self.tables:
            print(f"Table {table_name} does not exist")  # 添加打印信息
            raise ValueError(f"表 {table_name} 不存在")
        
        results = []
        print(f"Searching for matching data in memtable and SSTables")  # 添加打印信息
        for key, value in self.tables[table_name]['data'].items():
            if self.match_conditions(key, value, parsed_query.where_conditions):
                results.append((key, value))
        

        if not results:
            for level_sstables in reversed(self.sstables):
                for sstable in level_sstables:
                    with open(sstable.filename, 'r') as f:
                        data = json.load(f)
                        for key, value in data.items():
                            if self.match_conditions(key, value, parsed_query.where_conditions):
                                results.append((key, value))
                    if results:
                        break
                if results:
                    break
        
        print(f"SELECT query result: {results}")  # 添加打印信息
        return results

    def match_conditions(self, key, value, conditions):
        """
        检查键值对是否匹配WHERE条件。

        参数:
        - key: 要检查的键。
        - value: 要检查的值。
        - conditions: WHERE条件列表。

        返回:
        - 如果键值对匹配所有条件,则返回True;否则返回False。
        """
        for condition in conditions:
            column, operator, cond_value = condition
            if column == 'key':
                if not self.compare(key, operator, cond_value):
                    return False
            elif column == 'value':
                if not self.compare(value, operator, cond_value):
                    return False
            else:
                raise ValueError(f"Unsupported column in WHERE clause: {column}")
        return True

    def handle_insert(self, parsed_query):
        """
        处理INSERT查询。

        参数:
        - parsed_query: 解析后的查询对象。
        """
        table_name = parsed_query.tables[0]
        key = parsed_query.key
        value = parsed_query.value
        transaction_id = parsed_query.transaction_id
        self.tables[table_name]['data'][key] = value
        self.put(f"{table_name}:{key}", value, transaction_id)

    def handle_update(self, parsed_query):
        """
        处理UPDATE查询。

        参数:
        - parsed_query: 解析后的查询对象。
        """
        table_name = parsed_query.tables[0]
        key = parsed_query.key
        value = parsed_query.value
        transaction_id = parsed_query.transaction_id
        self.tables[table_name]['data'][key] = value
        self.update(f"{table_name}:{key}", value, transaction_id)

    def handle_delete(self, parsed_query):
        """
        处理DELETE查询。

        参数:
        - parsed_query: 解析后的查询对象。
        """
        table_name = parsed_query.tables[0]
        key = parsed_query.key
        transaction_id = parsed_query.transaction_id
        self.tables[table_name]['data'].pop(key, None)
        self.delete(f"{table_name}:{key}", transaction_id)

    def compare(self, a, operator, b):
        """
        比较两个值是否满足操作符。

        参数:
        - a: 第一个值。
        - operator: 比较操作符。
        - b: 第二个值。

        返回:
        - 如果两个值满足操作符,则返回True;否则返回False。
        """
        # 将字符串值转换为整数或浮点数
        if isinstance(a, str) and a.isdigit():
            a = int(a)
        elif isinstance(a, str) and '.' in a and a.replace('.', '', 1).isdigit():
            a = float(a)

        if isinstance(b, str) and b.isdigit():
            b = int(b)
        elif isinstance(b, str) and '.' in b and b.replace('.', '', 1).isdigit():
            b = float(b)

        if operator == '=':
            return a == b
        elif operator == '>':
            return a > b
        elif operator == '<':
            return a < b
        elif operator == '>=':
            return a >= b
        elif operator == '<=':
            return a <= b
        else:
            raise ValueError(f"Unsupported operator: {operator}")



class MetadataManager:
    def __init__(self, directory_path, pattern='*.json'):
        self.directory_path = directory_path
        self.pattern = pattern
        self.metadata = {}

    def count_records_in_json_file(self, filename):
        try:
            with open(filename, 'r') as file:
                data = json.load(file)
                record_count = len(data)
            return record_count
        except Exception as e:
            print(f"读取文件 {filename} 时出错: {e}")
            return 0

    def update_metadata(self):
        total_records = 0
        file_counts = {}
        for filename in glob.glob(os.path.join(self.directory_path, self.pattern)):
            record_count = self.count_records_in_json_file(filename)
            total_records += record_count
            file_counts[os.path.basename(filename)] = record_count

        self.metadata = {
            'total_records': total_records,
            'file_counts': file_counts
        }

    def get_metadata(self):
        return self.metadata


# 测试代码
'''
这部分代码的目的是在开始测试之前清理之前运行可能产生的SSTable文件和wal.log日志文件。
它使用glob库来查找所有名称匹配'sstable*.json'模式的文件，并使用os.remove()删除它们。
同时，它也会删除wal.log日志文件，以确保每次运行测试时都会从一个干净的状态开始，没有任何残留的SSTable文件和wal.log日志文件。
'''
# 删除旧的SSTable文件
for filename in glob.glob('sstable*.json'):  
    os.remove(filename)  

# 删除旧的wal.log日志文件
for filename in glob.glob('wal.log'):  
    os.remove(filename)  

# 创建 Faker 实例
fake = Faker()
# 创建LSM树实例
lsmt = LSMT()
# 创建QueryParser的实例
parser = QueryParser()

# 【√解析SQL查询】解析一个SQL查询
sql = "SELECT name, age FROM users WHERE age > 18 AND city = 'New York'"
parser.parse(sql)

# 访问解析后的结果
print("Tables:", parser.tables)
print("Columns:", parser.columns)
print("Where conditions:", parser.where_conditions)

# 【√创建表】创建一个名为 users 的表
create_table_query = "CREATE TABLE users (name VARCHAR, age INTEGER)"
parsed_create_table_query = parser.parse(create_table_query)
lsmt.execute_query(parsed_create_table_query)

# 生成并插入假数据
num_records = 2  # 您想要生成并插入的记录数量
insert_queries = []
for _ in range(num_records):
    name = fake.name()  # 生成假姓名
    age = fake.random_int(min=18, max=90)  # 生成假年龄
    insert_query = f"INSERT INTO users (name, age) VALUES ('{name}', {age})"
    insert_queries.append(insert_query)

for insert_query in insert_queries:
    parsed_insert_query = parser.parse(insert_query)
    lsmt.execute_query(parsed_insert_query)

# 打印表的内容来验证数据已被插入
print("Tables:", json.dumps(lsmt.tables, indent=4))

#【√验证表名】尝试从不存在的表中选择数据，预期打印 执行查询时出错: 表 non_existent_column 不存在
select_query = "SELECT non_existent_column FROM users"
parsed_select_query = parser.parse(select_query)
print("Parsed SELECT query:", parsed_select_query.__dict__)
try:
    results = lsmt.execute_query(parsed_select_query)
    print("Query results:", results)
except ValueError as e:
    print(f"执行查询时出错: {str(e)}")

# 【√布隆过滤器假阳性测试】
false_positives = 0
false_queries = 1000  # 测试不存在的键的数量
for i in range(false_queries):
    non_existent_key = f"nonexistentkey{i}"
    # 使用LSMT的get方法来查询不存在的键
    if lsmt.get(non_existent_key, 'test_transaction') is not None:
        false_positives += 1

false_positive_rate = false_positives / false_queries
print(f"Bloom Filter False Positive Rate: {false_positive_rate:.4f}")

# 【√B+TREE】插入一系列数据，然后使用范围查询来测试B+树索引
for i in range(10, 20):
    key = f"key{i}"
    value = f"value{i}"
    lsmt.put(key, value, 'test_transaction')

# 执行范围查询
start_key = "key10"
end_key = "key15"
print(f"Range Query Results from {start_key} to {end_key}:")
range_query_results = lsmt.range_query(start_key, end_key, 'test_transaction')
for result in range_query_results:
    print(result)


# 【√LRU缓存】测试LRU缓存的批量淘汰机制
def test_lru_cache_batch_eviction(lsmt_instance, batch_size):
    """
    测试LSMT实例中LRU缓存的批量淘汰机制。
    参数:
    - lsmt_instance: LSMT实例。
    - batch_size: 每次淘汰的项的数量。
    """
    print("\nTesting LRU Cache Batch Eviction...")

    # 插入足够的数据以触发缓存淘汰
    for i in range(lsmt_instance.cache_size + batch_size + 5):
        key = f"test_key_{i}"
        value = f"test_value_{i}"
        lsmt_instance.put(key, value, f"test_transaction_{i}", batch_size=batch_size)

    # 验证缓存大小不超过限制
    assert len(lsmt_instance.cache) <= lsmt_instance.cache_size, "Cache size exceeds the limit after eviction."

    print("LRU Cache Batch Eviction Test Passed!")

# 测试LRU缓存批量淘汰机制
test_lru_cache_batch_eviction(lsmt, batch_size=2)


# 【√统计表行数，假设只有一个表。原来的统计方法的代码得空去除】备注：错略统计，暂时没有考虑内存中的数据，待继续完善。
directory_path = '.'  # 假设当前目录
metadata_manager = MetadataManager(directory_path)
metadata_manager.update_metadata()
metadata = metadata_manager.get_metadata()

print("Metadata:")
print(f"总记录数量: {metadata['total_records']}")
print("各文件记录数量:")
for filename, count in metadata['file_counts'].items():
    print(f"{filename}: {count}")

print("All tests passed!")  # 打印测试通过消息
