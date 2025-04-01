# filter_useful_data_to_dict.py
from database_of_messages import async_main
from typing import List, Dict
import re
import asyncio
import json
import datetime  # 新增时间处理模块

async def fetch_target_messages(target_ids: List[int], db_config: dict) -> Dict[str, Dict]:
    """核心函数1：获取指定message_id的原始数据"""
    print("🛜 正在获取目标消息原始数据...")
    
    all_data = await async_main(
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
        # 处理时间格式
        date = details.get("date", "null")
        if isinstance(date, datetime.datetime):
            date = date.strftime("%Y-%m-%dT%H:%M:%S")  # 强制标准化格式
        
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

# 在validate_format函数中添加验证
def validate_format(formatted_data: Dict[str, str], target_ids: List[int]) -> bool:
    # ...原有验证不变...
    for msg_id, content in formatted_data.items():
        # 新增格式验证
        if "内容源于" in content and re.search(r'内容源于‘(\d+)’', content):
            raise ValueError(f"❌ 值 {msg_id} 包含数字来源标识")
    return True

async def get_formatted_data(db_config: dict, target_ids: List[int]) -> Dict[str, str]:
    """供其他模块调用的异步接口"""
    raw_data = await fetch_target_messages(target_ids, db_config)
    formatted_data = format_messages(raw_data)
    validate_format(formatted_data, target_ids)
    return formatted_data

async def main(db_config: dict, target_ids: List[int]):
    raw_data = await fetch_target_messages(target_ids, db_config)
    formatted_data = format_messages(raw_data)
    print_results(raw_data, formatted_data)

def print_results(raw_data: Dict[str, Dict], formatted_data: Dict[str, str]):
    print("\n=== 原始数据 ===")
    print(json.dumps(raw_data, ensure_ascii=False, indent=2))
    print("\n=== 格式化数据 ===")
    print(json.dumps(formatted_data, ensure_ascii=False, indent=2))

if __name__ == '__main__':
    DB_CONFIG = {
        "host": "103.116.245.150",
        "database": "ToDoAgent",
        "password": "4bc6bc963e6d8443453676"
    }
    TARGET_IDS = [
        327163713, 325202761, 325202741, 325151109,
        325151100, 325145820, 325144014, 324204487, 322085363
    ]
    asyncio.run(main(DB_CONFIG, TARGET_IDS))