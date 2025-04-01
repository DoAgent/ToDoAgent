'''
Description: 
Author: Manda
Version: 
Date: 2025-03-30 16:42:47
LastEditors: mdhuang555 67590178+mdhuang555@users.noreply.github.com
LastEditTime: 2025-03-30 16:59:19
'''
import mysql.connector
import os
from datetime import datetime
from collections import defaultdict

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

def get_time_slot(hour: int, minute: int) -> str:
    """将时间转换为40分钟一段的时间段"""
    # 计算一天中的第几个40分钟
    total_minutes = hour * 60 + minute
    slot_index = total_minutes // 40
    
    # 计算时间段的起始和结束时间
    start_minutes = slot_index * 40
    end_minutes = start_minutes + 40
    
    start_hour = start_minutes // 60
    start_minute = start_minutes % 60
    end_hour = end_minutes // 60
    end_minute = end_minutes % 60
    
    # 格式化时间段字符串
    return f"{start_hour:02d}:{start_minute:02d}-{end_hour:02d}:{end_minute:02d}"

def analyze_time_slots(conn: mysql.connector.MySQLConnection) -> dict:
    """分析时间段分布"""
    try:
        cursor = conn.cursor(dictionary=True)
        
        # 获取UCtodolist的数据和对应的ToDoList用户ID
        query = """
        SELECT uc.todo_id, uc.last_modified, t.user_id
        FROM UCtodolist uc
        JOIN ToDoList t ON uc.todo_id = t.todo_id
        WHERE uc.last_modified IS NOT NULL
        """
        cursor.execute(query)
        results = cursor.fetchall()
        
        # 按用户ID分组统计时间段
        user_time_slots = defaultdict(lambda: defaultdict(int))
        
        for row in results:
            if isinstance(row['last_modified'], datetime):
                hour = row['last_modified'].hour
                minute = row['last_modified'].minute
                time_slot = get_time_slot(hour, minute)
                user_time_slots[row['user_id']][time_slot] += 1
        
        return dict(user_time_slots)
    
    except Exception as e:
        print(f"分析时间段时出错: {e}")
        return {}
    finally:
        cursor.close()

def save_analysis_results(results: dict, output_dir: str = 'time_analysis'):
    """保存分析结果到文件"""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    for user_id, time_slots in results.items():
        filename = os.path.join(output_dir, f'user_{user_id}_time_analysis.txt')
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(f"分析时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"用户ID: {user_id}\n")
                f.write("=" * 50 + "\n\n")
                
                f.write("时间段使用频率统计（前6名）：\n")
                # 按频率排序并获取前6个时段
                top_slots = sorted(time_slots.items(), key=lambda x: x[1], reverse=True)[:6]
                
                for i, (slot, count) in enumerate(top_slots, 1):
                    f.write(f"第{i}名: {slot}\n")
                    f.write(f"  出现次数: {count}\n")
                    percentage = (count / sum(time_slots.values())) * 100
                    f.write(f"  占比: {percentage:.2f}%\n")
                    f.write("-" * 30 + "\n")
                
                # 添加总计信息
                f.write(f"\n总修改次数: {sum(time_slots.values())}\n")
                f.write(f"总时间段数: {len(time_slots)}/36\n")
                
            print(f"已保存用户 {user_id} 的时间分析到文件: {filename}")
            
        except Exception as e:
            print(f"保存用户 {user_id} 的分析结果时出错: {e}")

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
        print("正在分析时间段分布...")
        results = analyze_time_slots(conn)
        
        if results:
            print(f"分析完成，共有 {len(results)} 个用户的数据")
            save_analysis_results(results)
            print("分析结果已保存到文件中")
        else:
            print("未找到可分析的数据")
            
    except Exception as e:
        print(f"处理过程中出错: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()