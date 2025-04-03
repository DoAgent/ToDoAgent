
# '''
# Author: mdhuang555 67590178+mdhuang555@users.noreply.github.com
# Date: 2025-03-30 16:09:29
# LastEditors: mdhuang555 67590178+mdhuang555@users.noreply.github.com
# LastEditTime: 2025-04-03 11:02:35
# FilePath: \Notify\db2txt.py
# Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
# '''
from dataBaseConnecter import DatabaseConnector
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
import os
from datetime import datetime

def get_database_text(table: str) -> list:
    """使用DatabaseConnector从数据库获取数据"""
    try:
        # 创建数据库连接器实例
        db_connector = DatabaseConnector()
        
        # 连接数据库
        conn = db_connector.connect_db()
        if not conn:
            print("无法连接到数据库")
            return []
            
        try:
            # 使用连接器的extract_text方法获取数据
            results = db_connector.extract_text(conn, table, '*')
            return results
        finally:
            conn.close()
            
    except Exception as e:
        print(f"获取数据时发生错误: {e}")
        return []

def save_todos_by_user(todos: list, output_dir: str = 'output'):
    """将待办事项按用户ID保存到不同的文本文件中"""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    if not todos:
        print("没有数据可以保存")
        return
    
    # 按用户ID分组
    user_todos = {}
    for todo in todos:
        user_id = str(todo['user_id'])
        if user_id not in user_todos:
            user_todos[user_id] = []
        user_todos[user_id].append(todo)
    
    # 为每个用户创建文件
    for user_id, user_todos_list in user_todos.items():
        filename = os.path.join(output_dir, f'{user_id}.txt')
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(f"导出时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"用户ID: {user_id}\n")
                f.write("=" * 50 + "\n\n")
                
                for todo in user_todos_list:
                    f.write("待办事项:\n")
                    for key, value in todo.items():
                        if value is not None:  # 只写入非空值
                            f.write(f"  {key}: {value}\n")
                    f.write("-" * 50 + "\n")
            print(f"已保存用户 {user_id} 的待办事项到文件: {filename}")
        except Exception as e:
            print(f"保存用户 {user_id} 的数据时出错: {e}")

def main():
    print("正在连接数据库...")
    todos = get_database_text('ToDoList')
    
    if todos:
        print(f"成功获取 {len(todos)} 条记录")
        save_todos_by_user(todos)
        print("所有数据已保存完成")
    else:
        print("未能获取到数据")

if __name__ == "__main__":
    main()