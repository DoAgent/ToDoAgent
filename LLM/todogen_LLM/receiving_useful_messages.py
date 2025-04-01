# receiving_useful_messages.py
import json
import asyncio
import os
from typing import Dict, Any
from filter_useful_data_to_dict import fetch_target_messages
from todogen_llm import process_data, load_formatted_data

DATA_DIR = r"D:\python_study\ILoveDo\data_use"
RESULT_FILE = os.path.join(DATA_DIR, "result1.json")

async def merge_multisource_data() -> Dict[str, Dict]:
    """合并并扁平化数据字段"""
    DB_CONFIG = {
        "host": "103.116.245.150",
        "database": "ToDoAgent",
        "password": "4bc6bc963e6d8443453676"
    }
    TARGET_IDS = [
        327163713, 325202761, 325202741, 325151109,
        325151100, 325145820, 325144014, 324204487, 322085363
    ]

    # 获取数据源
    llm_formatted = await load_formatted_data()
    llm_processed = process_data(llm_formatted)
    db_raw = await fetch_target_messages(TARGET_IDS, DB_CONFIG)

    # 扁平化合并逻辑
    merged = {}
    for msg_id in TARGET_IDS:
        str_id = str(msg_id)
        raw_entry = db_raw.get(str_id, {})
        llm_entry = llm_processed.get(str_id, {})
        
        # 合并字段（LLM结构化数据优先覆盖原始数据）
        merged_entry = {**raw_entry, **llm_entry}  # 关键修改点
        
        if merged_entry:
            merged[str_id] = merged_entry

    return merged

async def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    
    merged_data = await merge_multisource_data()
    
    # 读取现有数据（兼容旧格式）
    try:
        with open(RESULT_FILE, "r", encoding="utf-8") as f:
            existing_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        existing_data = {}

    # 合并策略：新数据完全覆盖旧数据
    final_data = {**existing_data, **merged_data}
    
    with open(RESULT_FILE, "w", encoding="utf-8") as f:
        json.dump(final_data, f, ensure_ascii=False, indent=2)
    
    print(f"✅ 扁平化合并完成！有效记录：{len(final_data)} 条")

if __name__ == "__main__":
    asyncio.run(main())