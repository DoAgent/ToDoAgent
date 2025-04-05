# export_todolist.py
import json
from pathlib import Path
import mysql.connector
from config_loader import get_mysql_config, get_paths
from datetime import datetime  # 新增导入
import sys

sys.stdout.reconfigure(encoding='utf-8')

def convert_datetime(obj):
    """自定义JSON序列化处理器"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

def export_todolist_to_json():
    """导出todolist表数据到JSON文件"""
    try:
        # 获取配置
        db_config = get_mysql_config()
        paths = get_paths()
        
        # 建立数据库连接
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        
        # 执行查询
        cursor.execute("SELECT * FROM todolist")
        results = cursor.fetchall()
        
        # 创建输出目录
        output_dir = Path(paths['data_dir'])
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # 保存文件（增加cls参数）
        output_path = output_dir / "todolist_export.json"
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, 
                     indent=2, 
                     ensure_ascii=False,
                     default=convert_datetime)  # 关键修改
            
        print(f"✅ 成功导出 {len(results)} 条记录到 {output_path}")

        return str(output_path)
        
    except mysql.connector.Error as err:
        print(f"[错误] 数据库错误: {err}")  # 移除了Unicode符号
    except Exception as e:
        print(f"[错误] 发生异常: {str(e)}")
        return None
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

if __name__ == "__main__":
    export_todolist_to_json()