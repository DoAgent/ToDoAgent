import mysql.connector
from datetime import datetime  # 导入 datetime 模块
import os
from pathlib import Path
import json

#Azure MySQL数据库连接
current_dir = Path(__file__).parent.absolute()
ssl_ca_path = current_dir / "DigiCertGlobalRootCA.crt.pem"

#写入json时对datetime类型进行序列化
def datetime_serializer(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()  # 将 datetime 转换为 ISO 8601 格式的字符串
    raise TypeError("Type not serializable")

try:
    # 建立数据库连接
    cnx = mysql.connector.connect(
        user="siyuwang541", 
        password="ToDoAgentASAP！1", 
        host="todoagent-databases.mysql.database.azure.com", 
        port=3306, 
        database="todoagent",
        ssl_ca=str(ssl_ca_path),
        ssl_disabled=False
    )

    print("数据库连接成功！")
    
    # 测试查询
    cursor = cnx.cursor()
    cursor.execute("SELECT * FROM Messages")
    # 获取表头（列名）
    columns = [desc[0] for desc in cursor.description]

    # 获取数据
    rows = cursor.fetchall()

    # 将表头和数据合并为字典列表
    data = [dict(zip(columns, row)) for row in rows]

    # 打印表头和数据
    print("表头:", columns)
    print("数据:")
    for row in data:
        print(row)

    # 将数据写入 JSON 文件
    with open("Messages.json", "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=4, default=datetime_serializer)  # datetime使用自定义序列化器

    # 关闭连接
    cursor.close()
    cnx.close()
    print("连接已正常关闭")
    
except mysql.connector.Error as err:
    print(f"数据库错误: {err}")
except Exception as e:
    print(f"发生异常: {str(e)}")

    