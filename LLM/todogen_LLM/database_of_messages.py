# database_of_messages.py
import mysql.connector
from datetime import datetime
from pathlib import Path
import sys
from config_loader import get_mysql_config
import concurrent.futures  # 必须添加的模块导入
from concurrent.futures import ThreadPoolExecutor  # 关键修复导入
from tqdm import tqdm

sys.stdout.reconfigure(encoding='utf-8')

current_dir = Path(__file__).parent.absolute()
ssl_ca_path = current_dir / "DigiCertGlobalRootCA.crt.pem"

def process_row(args):
    """多线程处理单行数据"""
    columns, row = args
    row_dict = {}
    for col_name, value in zip(columns, row):
        if isinstance(value, datetime):
            row_dict[col_name] = value.isoformat()
        elif isinstance(value, int):
            row_dict[col_name] = str(value)
        else:
            row_dict[col_name] = str(value)
    message_id = str(row_dict.get("message_id", ""))
    return (message_id, row_dict)

def async_main(host: str, database: str, password: str) -> dict:
    """带进度条和多线程的版本"""
    db_config = get_mysql_config()  # 从配置加载
    try:
        conn = mysql.connector.connect(
            user=db_config['user'],
            password=db_config['password'],
            host=db_config['host'],
            port=db_config['port'],
            database=db_config['database'],
            ssl_ca=db_config['ssl_ca'],
            ssl_disabled=False
        )

        print("✅ 数据库连接成功")
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM Messages")
        result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        data = {}
        # 修复缩进问题：with语句后的代码块必须缩进
        with ThreadPoolExecutor() as executor, \
             tqdm(total=len(result), desc="数据获取进度") as pbar:  # 移除非ASCII符号
            
            future_to_row = {
                executor.submit(process_row, (columns, row)): row
                for row in result
            }
            
            # 使用完整模块路径
            for future in concurrent.futures.as_completed(future_to_row):
                message_id, row_dict = future.result()
                data[message_id] = row_dict
                pbar.update(1)  # 确保这里在with块内

        cursor.close()
        conn.close()
        return data

    except Exception as e:
        print(f"❌ 数据库错误: {str(e)}")
        return {}
    
"""
后面部分即为上传数据部分,切不可搞错

"""
def upload_to_todolist(data: dict):
    """将转换后的JSON数据上传到todolist表"""
    # 数据库配置（与database_of_messages.py保持一致）

    db_config = get_mysql_config()  # 从配置加载
    db_config = {
        'host':db_config['host'],
        'database':db_config['database'],
        'user':db_config['user'],
        'password':db_config['password'],
        'ssl_ca':db_config['ssl_ca']
    }

    # 读取转换后的数据 this is a test of editing file on github page
    # with open(json_path, 'r', encoding='utf-8') as f:
        # data = json.load(f)

    try:
        # 建立数据库连接
        cnx = mysql.connector.connect(**db_config)
        cursor = cnx.cursor()

        # 预处理插入语句
        insert_query = """
        INSERT INTO todolist (
            user_id, 
            start_time, 
            end_time, 
            location, 
            todo_content,
            todo_statu,
            urgency_statu
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        # 遍历数据并插入
        for item in data.values():
            # 数据转换逻辑
            try:
                # 必填字段处理
                todo_id = int(item["todo_id"])  # 必须为数字
                user_id = int(item["user_id"])  # 必须为数字
                start_time = datetime.fromisoformat(item["date"].replace("T", " "))
                todo_content = item["todo_content"]
                
                # 选填字段处理
                end_time = datetime.fromisoformat(item["end_time"]) if item.get("end_time") else None
                location = item.get("location", "")[:255]  # 截断超长内容
                
                # 使用默认值
                todo_status = item.get("todo_statu", "doing")
                urgency_status = item.get("urgency_statu", "unimportant")

                # 执行插入
                cursor.execute(insert_query, (
                    todo_id,
                    user_id,
                    start_time,
                    end_time,
                    location,
                    todo_content,
                    todo_status,
                    urgency_status
                ))
                
            except (KeyError, ValueError) as e:
                print(f"⚠️ 跳过无效数据 {item.get('todo_id')}: {str(e)}")
                continue

        # 提交事务
        cnx.commit()
        print(f"✅ 成功插入 {cursor.rowcount} 条记录")

    except mysql.connector.Error as err:
        print(f"❌ 数据库错误: {err}")
    finally:
        cursor.close()
        cnx.close()
