'''
Description: 
Author: Manda
Version: 
Date: 2025-03-30 16:28:58
LastEditors: mdhuang555 67590178+mdhuang555@users.noreply.github.com
LastEditTime: 2025-03-30 16:39:18
'''
import mysql.connector
import os
from datetime import datetime

def connect_to_database(db_config: dict) -> mysql.connector.MySQLConnection:
    """连接到MySQL数据库"""
    try:
        conn = mysql.connector.connect(
            host=db_config['host'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database'],
            charset='utf8mb4'
        )
        return conn
    except Exception as e:
        print(f"数据库连接错误: {e}")
        return None

def get_table_data(conn: mysql.connector.MySQLConnection, table_name: str) -> dict:
    """获取表格数据，以todo_id为键"""
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(f"SELECT * FROM {table_name}")
        results = cursor.fetchall()
        # 将结果转换为以todo_id为键的字典
        return {str(row['todo_id']): row for row in results}
    except Exception as e:
        print(f"获取{table_name}数据错误: {e}")
        return {}
    finally:
        cursor.close()

def compare_records(todolist_record: dict, uctodolist_record: dict) -> dict:
    """比较两条记录的差异"""
    differences = {}
    fields_to_compare = ['start_time', 'end_time', 'location', 'todo_content']
    
    for field in fields_to_compare:
        todo_value = todolist_record.get(field)
        uc_value = uctodolist_record.get(field)
        
        # 特殊处理datetime类型的比较
        if isinstance(todo_value, datetime):
            todo_value = todo_value.strftime('%Y-%m-%d %H:%M:%S')
        if isinstance(uc_value, datetime):
            uc_value = uc_value.strftime('%Y-%m-%d %H:%M:%S')
            
        if todo_value != uc_value:
            differences[field] = {
                'ToDoList': todo_value,
                'UCtodolist': uc_value
            }
    
    return differences

def save_differences_to_file(differences: dict, output_dir: str = 'compare_output'):
    """将差异保存到文件中"""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # 按用户ID分组
    user_differences = {}
    for todo_id, diff in differences.items():
        user_id = diff['user_id']
        if user_id not in user_differences:
            user_differences[user_id] = {}
        user_differences[user_id][todo_id] = diff['differences']

    # 为每个用户创建文件
    for user_id, user_diffs in user_differences.items():
        filename = os.path.join(output_dir, f'user_{user_id}_differences.txt')
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(f"对比时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"用户ID: {user_id}\n")
            f.write("=" * 50 + "\n\n")
            
            for todo_id, diffs in user_diffs.items():
                f.write(f"待办事项ID: {todo_id}\n")
                for field, values in diffs.items():
                    f.write(f"  字段: {field}\n")
                    f.write(f"    ToDoList值: {values['ToDoList']}\n")
                    f.write(f"    UCtodolist值: {values['UCtodolist']}\n")
                f.write("-" * 50 + "\n")
        
        print(f"已保存用户 {user_id} 的差异到文件: {filename}")

def main():
    db_config = {
        'host': '103.116.245.150',
        'user': 'root',
        'password': '4bc6bc963e6d8443453676',
        'database': 'ToDoAgent'
    }

    print("正在连接数据库...")
    conn = connect_to_database(db_config)
    if not conn:
        print("数据库连接失败")
        return

    try:
        # 获取两个表的数据
        print("正在获取表格数据...")
        todolist_data = get_table_data(conn, 'ToDoList')
        uctodolist_data = get_table_data(conn, 'UCtodolist')

        # 比较差异
        print("正在比较差异...")
        differences = {}
        for todo_id in set(todolist_data.keys()) & set(uctodolist_data.keys()):
            todolist_record = todolist_data[todo_id]
            uctodolist_record = uctodolist_data[todo_id]
            
            record_differences = compare_records(todolist_record, uctodolist_record)
            if record_differences:
                differences[todo_id] = {
                    'user_id': todolist_record['user_id'],
                    'differences': record_differences
                }

        # 保存差异
        if differences:
            print(f"发现 {len(differences)} 条记录有差异")
            save_differences_to_file(differences)
            print("差异已保存到文件中")
        else:
            print("未发现差异")

    except Exception as e:
        print(f"处理过程中出错: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()