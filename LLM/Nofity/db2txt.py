import mysql.connector
import os
from datetime import datetime

def get_database_text(db_config: dict, table: str) -> list:
    """直接从MySQL数据库获取数据"""
    try:
        # 直接连接MySQL数据库
        conn = mysql.connector.connect(
            host=db_config['host'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database'],
            charset='utf8mb4'
        )
        
        cursor = conn.cursor(dictionary=True)
        
        # 获取所有待办事项
        cursor.execute(f"SELECT * FROM {table}")
        results = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return results
        
    except mysql.connector.Error as e:
        print(f"数据库错误: {e}")
        return []
    except Exception as e:
        print(f"其他错误: {e}")
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
    # 数据库配置
    db_config = {
        'host': '103.116.245.150',
        'user': 'root',
        'password': '4bc6bc963e6d8443453676',
        'database': 'ToDoAgent'
    }

    print("正在连接数据库...")
    todos = get_database_text(db_config, 'ToDoList')
    
    if todos:
        print(f"成功获取 {len(todos)} 条记录")
        save_todos_by_user(todos)
        print("所有数据已保存完成")
    else:
        print("未能获取到数据")

if __name__ == "__main__":
    main()