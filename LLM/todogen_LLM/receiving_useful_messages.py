# receiving_useful_messages.py
import json
import os
from typing import Dict, Any
from pathlib import Path
from config_loader import get_paths,get_mysql_config
from filter_message_list import get_message_ids
from filter_useful_data_to_dict import get_formatted_data  # 改为导入顶层接口
from todogen_llm import process_data

DATA_DIR = get_paths()['data_dir']
RESULT_FILE = os.path.join(DATA_DIR, get_paths()['result_file'])

# receiving_useful_messages.py
def merge_multisource_data() -> Dict[str, Dict]:
    """合并并扁平化数据字段（同步版本）- 修复版"""
    db_config = get_mysql_config()
    DB_CONFIG = {
        'host':db_config['host'],
        'database':db_config['database'],
        'password':db_config['password']
    }
    
    # 严格ID验证
    raw_ids = get_message_ids()
    # if len(raw_ids) != 24:
        # raise ValueError(f"目标ID数量异常，预期24条，实际获取{len(raw_ids)}条")
    target_ids = [str(msg_id) for msg_id in raw_ids]

    # 数据源获取与验证
    llm_formatted = get_formatted_data(DB_CONFIG, raw_ids)  # 注意传入原始ID


    llm_processed = process_data(llm_formatted)


    # 精确获取原始数据
    from database_of_messages import async_main
    db_raw = async_main(**DB_CONFIG)
    
    # 双重过滤保证数据纯净度
    filtered_db_data = {k: v for k, v in db_raw.items() if k in target_ids}

    # 安全合并逻辑
    merged = {}
    for msg_id in target_ids:
        # 确保只合并目标ID的数据
        base_data = filtered_db_data.get(msg_id, {})
        llm_data = llm_processed.get(msg_id, {})
        
        # 空值过滤
        if not base_data and not llm_data:
            continue
            
        merged_entry = {**base_data, **llm_data}
        merged[msg_id] = merged_entry

    return merged

def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    
    merged_data = merge_multisource_data()

    """根据M、D的需要，这里需要按照unimportant =1 ,important =2 ;urgency =3进行转换"""
    # 新增：转换urgency字段
    for entry in merged_data.values():
        urgency = entry.get("urgency", "").lower()
        if urgency == "unimportant":
            entry["urgency"] = 1
        elif urgency == "important":
            entry["urgency"] = 2
        elif urgency == "urgent":
            entry["urgency"] = 3

    # 修改：将字典的值提取为列表
    final_data_list = list(merged_data.values())
    
    with open(RESULT_FILE, "w", encoding="utf-8") as f:
        json.dump(final_data_list, f, ensure_ascii=False, indent=2)
    
    print(f"✅ 扁平化合并完成！有效记录：{len(final_data_list)} 条")

    return RESULT_FILE # 返回生成的 result1.json 路径（如 data/result1.json）

if __name__ == "__main__":
    main()


"""
if  content +sender 不相同:
 直接上传
elif user 相同：
 需要进行比对，然后上传某一条数据
"""