import json  # 导入JSON库，用于键值对的序列化和反序列化

# KeyValue类代表了一个键值对。这是LSM-tree存储引擎的基本数据单位。
# 该类提供了基本的构造、比较和表示方法，并通过序列化和反序列化方法支持JSON格式的存储。
class KeyValue:
    def __init__(self, key, value):  # 构造函数，初始化键值对
        self.key = key   # 键，可以是任何可哈希的类型
        self.value = value # 值，可以是任何类型

    def __repr__(self):  # 返回对象的字符串表示，有助于调试和日志记录
        return f"Key: {self.key}, Value: {self.value}"

    def __eq__(self, other):  # 定义等式检查，用于比较键值对是否相等
        if isinstance(other, KeyValue): # 确保另一个对象是键值对类型
            return self.key == other.key and self.value == other.value # 检查键和值是否相等
        return False # 如果另一个对象不是键值对类型，则返回False

    def __lt__(self, other):  # 定义小于比较，用于排序键值对
        return str(self.key) < str(other.key)  # 将键转换为字符串后进行比较

    def serialize(self) -> str:  # 序列化方法，将键值对转换为JSON字符串
        # 针对非基本数据类型转换为字符串，基本类型保持不变
        key = str(self.key) if not isinstance(self.key, (int, float, str, list, dict)) else self.key
        value = str(self.value) if not isinstance(self.value, (int, float, str, list, dict)) else self.value
        return json.dumps({"key": key, "value": value})  # 使用json.dumps将键值对转换为JSON

    @staticmethod  # 静态方法，不依赖于类的实例
    def deserialize(serialized: str):  # 反序列化方法，从JSON字符串创建KeyValue对象
        data = json.loads(serialized)  # 使用json.loads解析JSON字符串
        return KeyValue(data["key"], data["value"])  # 从解析的数据中提取键和值，并创建新的KeyValue对象

