# '''
# Description: 
# Author: Manda
# Version: 
# Date: 2025-03-30 16:28:58
# LastEditors: mdhuang555 67590178+mdhuang555@users.noreply.github.com
# LastEditTime: 2025-03-30 16:39:18
# '''
from dataBaseConnecter import DatabaseConnector
import os
from datetime import datetime
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

def get_table_data(db_connector: DatabaseConnector, table_name: str) -> dict:
    """获取表格数据，以todo_id为键"""
    try:
        # 连接数据库
        conn = db_connector.connect_db()
        if not conn:
            print("无法连接到数据库")
            return {}
            
        cursor = conn.cursor(dictionary=True)
        try:
            # 使用连接器的extract_text方法获取数据
            results = db_connector.extract_text(conn, table_name, '*')
            # 将结果转换为以todo_id为键的字典
            return {str(row['todo_id']): row for row in results}
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        print(f"获取{table_name}数据错误: {e}")
        return {}

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
    print("正在连接数据库...")
    
    try:
        # 创建数据库连接器实例
        db_connector = DatabaseConnector()
        
        # 获取两个表的数据
        print("正在获取表格数据...")
        todolist_data = get_table_data(db_connector, 'ToDoList')
        uctodolist_data = get_table_data(db_connector, 'UCtodolist')

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

if __name__ == "__main__":
    main()