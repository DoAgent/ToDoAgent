# filter_useful_data_to_dict.py
from database_of_messages import async_main  # 确保导入的是同步函数
from typing import List, Dict
import re
import json
import datetime
from filter_message_list import get_message_ids

# 移除所有异步装饰器和await调用
def fetch_target_messages(target_ids: List[int], db_config: dict) -> Dict[str, Dict]:
    """核心函数1：获取指定message_id的原始数据"""
    print("🛜 正在获取目标消息原始数据...")
    
    # 同步调用
    all_data = async_main(
        host=db_config["host"],
        database=db_config["database"],
        password=db_config["password"]
    )
    
    str_ids = {str(msg_id) for msg_id in target_ids}
    filtered_data = {k: v for k, v in all_data.items() if k in str_ids}
    
    print(f"✅ 找到 {len(filtered_data)}/{len(target_ids)} 条目标消息")
    return filtered_data

def format_messages(raw_data: Dict[str, Dict]) -> Dict[str, str]:
    """核心函数2：格式化消息为指定字符串"""
    print("\n🔄 正在进行数据格式化...")
    
    formatted = {}
    for msg_id, details in raw_data.items():
        # 修正：date字段在database_of_messages中已被转换为字符串
        date = details.get("date", "null")
        if isinstance(date, str):  # 类型检查改为字符串
            try:
                # 转换为datetime对象确保格式有效
                parsed_date = datetime.datetime.fromisoformat(date)
                date = parsed_date.strftime("%Y-%m-%dT%H:%M:%S")
            except ValueError:
                date = "null"
        
        sender = details.get("sender", "null").strip("'‘’")
        content = details.get("content", "null")
        
        # 判断sender是否为纯数字
        if sender.isdigit():
            formatted_str = f"开始日期为{date}，{content}"
        else:
            formatted_str = f"开始日期为{date}，内容源于‘{sender}’，{content}"
        
        formatted[msg_id] = formatted_str
    
    print("🎉 格式化完成")
    return formatted

def validate_format(formatted_data: Dict[str, str], target_ids: List[int]) -> bool:
    for msg_id, content in formatted_data.items():
        if "内容源于" in content and re.search(r'内容源于‘(\d+)’', content):
            raise ValueError(f"❌ 值 {msg_id} 包含数字来源标识")
    return True

# 改为同步接口
def get_formatted_data(db_config: dict, target_ids: List[int]) -> Dict[str, str]:
    """供其他模块调用的同步接口"""
    raw_data = fetch_target_messages(target_ids, db_config)
    formatted_data = format_messages(raw_data)
    validate_format(formatted_data, target_ids)
    return formatted_data

def main(db_config: dict, target_ids: List[int]):
    raw_data = fetch_target_messages(target_ids, db_config)
    formatted_data = format_messages(raw_data)
    print_results(raw_data, formatted_data)

def print_results(raw_data: Dict[str, Dict], formatted_data: Dict[str, str]):
    print("\n=== 原始数据 ===")
    print(json.dumps(raw_data, ensure_ascii=False, indent=2))
    print("\n=== 格式化数据 ===")
    print(json.dumps(formatted_data, ensure_ascii=False, indent=2))

if __name__ == '__main__':
    DB_CONFIG = {
        "host": "todoagent-databases.mysql.database.azure.com",  # 修正为正确的host
        "database": "todoagent",
        "password": "ToDoAgentASAP！1"  # 保持中文感叹号
    }
    TARGET_IDS = get_message_ids()
    main(DB_CONFIG, TARGET_IDS)  # 直接同步调用