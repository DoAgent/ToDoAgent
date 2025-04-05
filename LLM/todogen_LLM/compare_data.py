import json
import os
import sys
import copy
from config_loader import get_paths
from datetime import datetime

from export_todolist import export_todolist_to_json
from receiving_useful_messages import main

# --- 配置与辅助函数 ---
sys.stdout.reconfigure(encoding='utf-8')

def convert_datetime(obj):
    """自定义JSON序列化处理器"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

def load_json_data(file_path):
    """加载JSON文件数据，处理错误并确保返回列表"""
    if not os.path.exists(file_path):
        print(f"[错误] 文件未找到: {file_path}")
        return None
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
        else:
            print(f"[错误] 文件格式不正确，预期为列表: {file_path}")
            return None
    except json.JSONDecodeError:
        print(f"[错误] JSON解码失败: {file_path}")
        return None
    except Exception as e:
        print(f"[错误] 加载文件时发生未知错误 {file_path}: {str(e)}")
        return None

def generate_unique_id(base_id, existing_ids_set):
    """生成唯一的 ID (base_id_upd, base_id_upd_1, ...)"""
    potential_id = f"{str(base_id)}_upd"
    counter = 1
    while potential_id in existing_ids_set:
        potential_id = f"{str(base_id)}_upd_{counter}"
        counter += 1
    return potential_id

# --- 核心处理逻辑函数 ---
def process_record(item_r, existing_message_ids, existing_todo_contents, existing_message_id_to_record, all_known_message_ids, stats):
    """
    处理来自 result1.json 的单条记录，根据规则决定操作。
    返回: 要添加到 compare.json 的记录 (字典) 或 None。
    同时更新 stats 字典和 all_known_message_ids 集合。
    """
    record_to_save = None # 初始化返回值

    try:
        r_message_id_str = str(item_r['message_id'])
        r_todo_content_str = str(item_r['todo_content'])
    except KeyError as e:
        print(f"[警告] result1.json 中的记录缺少键 {e}，已跳过: {item_r}")
        stats['skipped_missing_keys'] += 1
        return None
    except TypeError as e:
         print(f"[警告] result1.json 中的记录键值类型错误 {e}，已跳过: {item_r}")
         stats['skipped_missing_keys'] += 1
         return None

    if r_message_id_str not in existing_message_ids:
        # --- 情况 1: 新 message_id ---
        if r_todo_content_str not in existing_todo_contents:
            # 1.1: 新 todo_content -> 保存
            record_to_save = item_r
            all_known_message_ids.add(r_message_id_str) # 追踪新 ID
            stats['saved_new_id_new_content'] += 1
        else:
            # 1.2: 已存在 todo_content -> 打印
            print("-" * 30)
            print(f"打印 (新 message_id: {r_message_id_str}, 但 todo_content 已存在):")
            print(json.dumps(item_r, indent=2, ensure_ascii=False, default=convert_datetime))
            print("-" * 30)
            stats['printed_new_id_existing_content'] += 1
    else:
        # --- 情况 2: 已存在 message_id ---
        record_e = existing_message_id_to_record.get(r_message_id_str) # 获取现有记录
        if record_e is None: 
            # 理论上不应发生，因为 ID 在 existing_message_ids 中
            print(f"[警告] ID {r_message_id_str} 在集合中但在字典中找不到？跳过。")
            stats['skipped_internal_error'] = stats.get('skipped_internal_error', 0) + 1 # 新增统计
            return None 
            
        e_todo_content_str = str(record_e.get('todo_content', '')) # 安全获取

        if r_todo_content_str != e_todo_content_str:
            # 2.1: todo_content 不同 -> 修改 ID 并保存
            new_unique_id = generate_unique_id(r_message_id_str, all_known_message_ids)
            # all_known_message_ids.add(new_unique_id) # 追踪新生成的 ID

            modified_item_r = copy.deepcopy(item_r)
            # modified_item_r['message_id'] = new_unique_id
            record_to_save = modified_item_r
            stats['saved_modified_id_diff_content'] += 1
        else:
            # 2.2: todo_content 相同 -> 打印
            print("-" * 30)
            print(f"打印 (message_id: {r_message_id_str} 已存在, todo_content 相同):")
            print("来自 result1.json:")
            print(json.dumps(item_r, indent=2, ensure_ascii=False, default=convert_datetime))
            print("-" * 30)
            stats['printed_existing_id_same_content'] += 1

    return record_to_save

# --- 主函数 ---
def compare_and_generate_updates():
    """主函数：加载数据、处理、保存和打印统计信息"""
    paths = get_paths()
    data_dir = paths['data_dir']

    # +++ 新增: 定义 compare_output_file 路径 +++
    compare_output_file = os.path.join(data_dir, "compare.json")  # 明确输出路径

     # 1. 主动触发数据导出流程，获取导出的 JSON 文件路径
    extracted_list_path = export_todolist_to_json()  # 返回 todolist_export.json 的路径
    if not extracted_list_path or not os.path.exists(extracted_list_path):
        print("[错误] 导出 todolist 数据失败，流程终止。")
        return

    # 2. 主动触发消息处理流程，生成 result1.json
    result1_path = main()  # 返回 result1.json 的路径
    if not result1_path or not os.path.exists(result1_path):
        print("[错误] 生成 result1.json 失败，流程终止。")
        return

    # 3. 加载数据
    result1_data = load_json_data(result1_path)
    extracted_data = load_json_data(extracted_list_path)
    if result1_data is None or extracted_data is None:
        print("[错误] 数据加载失败，流程终止。")
        return

    # --- 创建查找结构 ---
    try:
        existing_message_ids = {str(item['message_id']) for item in extracted_data if 'message_id' in item}
        existing_message_id_to_record = {str(item['message_id']): item for item in extracted_data if 'message_id' in item}
        existing_todo_contents = {str(item['todo_content']) for item in extracted_data if 'todo_content' in item}
    except (KeyError, TypeError) as e:
        print(f"[错误] extracted_list.json 文件处理失败: {e}。请检查文件内容和格式。")
        return

    # --- 初始化 ---
    records_for_compare_json = []
    all_known_message_ids = set(existing_message_ids)
    stats = { # 使用字典来存储统计数据
        'processed': 0,
        'skipped_missing_keys': 0,
        'saved_new_id_new_content': 0,
        'printed_new_id_existing_content': 0,
        'saved_modified_id_diff_content': 0,
        'printed_existing_id_same_content': 0,
        'skipped_internal_error': 0 # 用于 process_record 内部错误
    }

    print("[信息] 开始比较和处理数据...")
    # --- 主循环 ---
    for item_r in result1_data:
        stats['processed'] += 1
        record_to_save = process_record(
            item_r,
            existing_message_ids,
            existing_todo_contents,
            existing_message_id_to_record,
            all_known_message_ids,
            stats # 传递 stats 字典用于更新
        )
        if record_to_save is not None:
            records_for_compare_json.append(record_to_save)

    # --- 写入文件 ---
    try:
        with open(compare_output_file, 'w', encoding='utf-8') as f:
            json.dump(records_for_compare_json, f, indent=2, ensure_ascii=False, default=convert_datetime)
        print(f"✅ 成功生成 compare.json 文件，包含 {len(records_for_compare_json)} 条记录。")
    except Exception as e:
        print(f"[错误] 写入 compare.json 文件时发生错误: {str(e)}")

    # --- 打印统计 ---
    print("=" * 40)
    print("处理统计:")
    print(f"  处理 result1.json 记录总数: {stats['processed']}")
    print(f"  跳过 (缺少关键键或类型错误): {stats['skipped_missing_keys']}")
    if stats['skipped_internal_error'] > 0:
       print(f"  跳过 (内部逻辑错误): {stats['skipped_internal_error']}")
    print("-" * 20)
    print("  写入 compare.json:")
    print(f"    - 新 message_id, 新 todo_content: {stats['saved_new_id_new_content']}")
    print(f"    - 修改后 message_id (因冲突且 todo_content 不同): {stats['saved_modified_id_diff_content']}")
    print("-" * 20)
    print("  打印到控制台:")
    print(f"    - 新 message_id, 但 todo_content 已存在: {stats['printed_new_id_existing_content']}")
    print(f"    - message_id 已存在, todo_content 相同: {stats['printed_existing_id_same_content']}")
    print("=" * 40)


    return records_for_compare_json

if __name__ == "__main__":
    compare_and_generate_updates()  # 触发整个流程