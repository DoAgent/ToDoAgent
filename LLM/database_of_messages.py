import mysql.connector
from mysql.connector import Error
from typing import List, Dict, Any, Optional
import sys
import json
from datetime import datetime
import re

# 设置输出编码
sys.stdout.reconfigure(encoding='utf-8')

class DatabaseConfig:
    """数据库配置类"""
    def __init__(self, host: str, database: str, password: str):
        self.config = {
            "host": host,
            "port": 3306,
            "database": database,
            "user": "root",
            "password": password,
            "charset": "utf8mb4",
            "use_unicode": True
        }

class DateTimeEncoder(json.JSONEncoder):
    """处理datetime对象的JSON编码器"""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

class DatabaseHandler:
    """数据库处理类"""
    def __init__(self, config: DatabaseConfig):
        self.config = config.config
        self.connection = None

    def connect(self) -> bool:
        """建立数据库连接"""
        try:
            self.connection = mysql.connector.connect(**self.config)
            if self.connection.is_connected():
                # 设置连接的字符编码
                cursor = self.connection.cursor(dictionary=True)
                cursor.execute('SET NAMES utf8mb4')
                cursor.execute('SET CHARACTER SET utf8mb4')
                cursor.execute('SET character_set_connection=utf8mb4')
                cursor.close()
                return True
            return False
        except Error as e:
            print(f"数据库连接错误: {e}")
            return False

    def get_all_tables(self) -> List[str]:
        """获取所有表名"""
        try:
            if not self.connection or not self.connection.is_connected():
                print("数据库未连接")
                return []
            
            cursor = self.connection.cursor(dictionary=True)
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
            cursor.close()
            return [table[list(table.keys())[0]] for table in tables]
        except Error as e:
            print(f"获取表名错误: {e}")
            return []

    def fetch_table_data(self, table_name: str) -> List[Dict]:
        """获取表数据"""
        try:
            if not self.connection or not self.connection.is_connected():
                print("数据库未连接")
                return []
            
            cursor = self.connection.cursor(dictionary=True)
            cursor.execute(f"SELECT * FROM `{table_name}`")
            rows = cursor.fetchall()
            cursor.close()
            return rows
        except Error as e:
            print(f"获取{table_name}表数据错误: {e}")
            return []

    def fetch_messages_data(self) -> List[Dict]:
        """专门获取Messages表的数据，只返回指定字段"""
        try:
            if not self.connection or not self.connection.is_connected():
                print("数据库未连接")
                return []
            
            cursor = self.connection.cursor(dictionary=True)
            cursor.execute("""
                SELECT 
                    sender,
                    content,
                    app_name,
                    message_id,
                    user_id,
                    date
                FROM Messages
            """)
            rows = cursor.fetchall()
            cursor.close()
            return rows
        except Error as e:
            print(f"获取Messages表数据错误: {e}")
            return []

    def close(self):
        """关闭数据库连接"""
        if self.connection and self.connection.is_connected():
            self.connection.close()

def main(host, database, password, result=None) -> List[Dict[str, Any]]:
    """主函数 - 返回消息列表   这个可以获取数据库中的全部消息"""
    
    # 初始化数据库处理器
    db_config = DatabaseConfig(
        host=host,
        database=database,
        password=password
    )
    db_handler = DatabaseHandler(db_config)
    
    # 连接数据库
    if not db_handler.connect():
        print("数据库连接失败")
        return []

    try:
        # 获取Messages表数据
        messages = db_handler.fetch_messages_data()
        return messages  # 直接返回消息列表

    except Error as e:
        print(f"处理数据时出错: {e}")
        return []
    
    finally:
        db_handler.close()


if __name__ == "__main__":
    data_result = main(
        host="103.116.245.150",
        database="ToDoAgent",
        password="4bc6bc963e6d8443453676"
    )

    print(json.dumps(data_result, ensure_ascii=False, indent=4, cls=DateTimeEncoder))








"""
 list_id 
 user_id 
 start_time 
 end_time
 location 
 todo_content
 last_modified
 done 

todo_content：待办事项内容
last_modified：最后修改时间
done：已完成
"""


