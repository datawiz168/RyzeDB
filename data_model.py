import json
from typing import Any

class KeyValue:
    def serialize(self) -> dict:
        return {'key': self.key, 'value': self.value}

    def __init__(self, key: Any, value: Any):
        # 构造函数，用于创建键值对对象
        # key: 键，可以是任何可哈希的类型
        # value: 值，可以是任何类型
        self.key = key
        self.value = value

    def __repr__(self) -> str:
        # 返回对象的字符串表示
        # 用于调试和日志记录
        return f"Key: {self.key}, Value: {self.value}"

    def __eq__(self, other: Any) -> bool:
        # 定义等式检查，用于比较两个键值对是否相等
        # other: 另一个要比较的对象
        # 返回: 如果键和值都相等，则为True，否则为False
        if isinstance(other, KeyValue):
            return self.key == other.key and self.value == other.value
        return False

    def __lt__(self, other: Any) -> bool:
        # 定义小于比较，用于排序键值对
        # other: 另一个要比较的对象
        # 返回: 如果当前对象的键小于另一个对象的键，则为True，否则为False
        return str(self.key) < str(other.key)

    def serialize(self) -> str:
        # 序列化方法，将键值对转换为JSON字符串
        # 返回: 表示键值对的JSON字符串
        key = self._serialize_value(self.key)
        value = self._serialize_value(self.value)
        return json.dumps({"key": key, "value": value})

    def _serialize_value(self, value: Any) -> Any:
        # 辅助方法，用于序列化单个值
        # value: 要序列化的值
        # 返回: 序列化后的值
        # 如果值是基本类型，则直接返回
        # 如果值是元组，则转换为列表
        # 如果值是集合，则转换为列表
        # 对于其他类型，转换为字符串
        if isinstance(value, (int, float, str, list, dict)):
            return value
        elif isinstance(value, tuple):
            return list(value)
        elif isinstance(value, set):
            return list(value)
        else:
            return str(value)

    @staticmethod
    def deserialize(serialized: str) -> 'KeyValue':
        try:
            # 尝试将输入的字符串解析为JSON对象
            # serialized参数应该是一个包含键和值的JSON字符串
            data = json.loads(serialized)

            # 从解析后的JSON对象中提取键和值
            # 并使用它们创建一个新的KeyValue对象
            # 然后返回这个新对象
            return KeyValue(data["key"], data["value"])
        except json.JSONDecodeError:
            # 如果输入的字符串不是有效的JSON格式
            # 则抛出ValueError异常，并附带解释信息
            raise ValueError("Invalid serialized string") from None
        except KeyError:
            # 如果解析后的JSON对象缺少键或值
            # 则抛出ValueError异常，并附带解释信息
            raise ValueError("Missing key or value in serialized string") from None

